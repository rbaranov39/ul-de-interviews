import json
import os
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2
from catboost import CatBoostRegressor
from psycopg2.extras import execute_values
from sklearn.metrics import (
    mean_absolute_error,
    mean_absolute_percentage_error,
    mean_squared_error,
    r2_score,
)


MODEL_NAME = "catboost_regressor_v1"
RANDOM_SEED = 42

CATEGORICAL_FEATURES = [
    "loading_city_name",
    "loading_region",
    "unloading_city_name",
    "unloading_region",
    "body_group",
    "weight_category",
    "volume_category",
]

NUMERIC_FEATURES = [
    "loading_latitude",
    "loading_longitude",
    "unloading_latitude",
    "unloading_longitude",
    "distance",
    "total_weight",
    "total_volume",
    "year",
    "month",
    "day_of_week",
    "day_of_year",
    "month_sin",
    "month_cos",
    "day_of_week_sin",
    "day_of_week_cos",
    "day_of_year_sin",
    "day_of_year_cos",
]

FEATURES = CATEGORICAL_FEATURES + NUMERIC_FEATURES


def get_db_config() -> dict[str, str | int]:
    return {
        "host": os.environ["ETL_DB_HOST"],
        "port": int(os.getenv("ETL_DB_PORT", "5432")),
        "dbname": os.environ["ETL_DB_NAME"],
        "user": os.environ["ETL_DB_USER"],
        "password": os.environ["ETL_DB_PASSWORD"],
    }


def load_dataset(connection: psycopg2.extensions.connection) -> pd.DataFrame:
    sql_path = (
        Path(__file__).resolve().parents[1]
        / "sql"
        / "dml"
        / "ml_features.sql"
    )
    query = sql_path.read_text(encoding="utf-8")

    with connection.cursor() as cursor:
        cursor.execute(query)
        columns = [description.name for description in cursor.description]
        rows = cursor.fetchall()

    return pd.DataFrame(rows, columns=columns)


def add_time_features(dataset: pd.DataFrame) -> pd.DataFrame:
    result = dataset.copy()
    loading_date = pd.to_datetime(result["loading_date"])

    result["year"] = loading_date.dt.year
    result["month"] = loading_date.dt.month
    result["day_of_week"] = loading_date.dt.dayofweek
    result["day_of_year"] = loading_date.dt.dayofyear

    result["month_sin"] = np.sin(2 * np.pi * result["month"] / 12)
    result["month_cos"] = np.cos(2 * np.pi * result["month"] / 12)
    result["day_of_week_sin"] = np.sin(
        2 * np.pi * result["day_of_week"] / 7
    )
    result["day_of_week_cos"] = np.cos(
        2 * np.pi * result["day_of_week"] / 7
    )
    result["day_of_year_sin"] = np.sin(
        2 * np.pi * result["day_of_year"] / 365.25
    )
    result["day_of_year_cos"] = np.cos(
        2 * np.pi * result["day_of_year"] / 365.25
    )

    for column in CATEGORICAL_FEATURES:
        result[column] = result[column].fillna("UNKNOWN").astype(str)

    for column in NUMERIC_FEATURES:
        result[column] = pd.to_numeric(result[column], errors="coerce")

    return result


def build_model() -> CatBoostRegressor:
    return CatBoostRegressor(
        iterations=400,
        depth=8,
        learning_rate=0.08,
        loss_function="RMSE",
        random_seed=RANDOM_SEED,
        thread_count=4,
        verbose=100,
        allow_writing_files=False,
    )


def calculate_metrics(
    actual: pd.Series,
    predicted: np.ndarray,
) -> dict[str, float]:
    return {
        "r2": float(r2_score(actual, predicted)),
        "mape_pct": float(
            mean_absolute_percentage_error(actual, predicted) * 100
        ),
        "rmse_rub": float(mean_squared_error(actual, predicted) ** 0.5),
        "mae_rub": float(mean_absolute_error(actual, predicted)),
    }


def nullable_rounded(value: float | None, digits: int) -> float | None:
    if value is None or pd.isna(value):
        return None
    return round(float(value), digits)


def save_predictions(
    connection: psycopg2.extensions.connection,
    scored: pd.DataFrame,
) -> int:
    scored_at = datetime.now(timezone.utc)
    rows = []

    for row in scored.itertuples(index=False):
        actual = nullable_rounded(row.actual_price_rub, 2)
        predicted = nullable_rounded(row.predicted_price_rub, 2)
        abs_error = None
        ape_pct = None

        if row.dataset_split == "test" and actual is not None:
            abs_error = round(abs(actual - predicted), 2)
            ape_pct = round(abs(actual - predicted) / actual * 100, 4)

        rows.append(
            (
                int(row.price_id),
                row.loading_date,
                int(row.loading_city_gcid),
                int(row.unloading_city_gcid),
                row.body_group,
                row.dataset_split,
                actual if row.dataset_split == "test" else None,
                predicted,
                abs_error,
                ape_pct,
                MODEL_NAME,
                scored_at,
            )
        )

    with connection.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE dm.ml_price_predictions")
        execute_values(
            cursor,
            """
            INSERT INTO dm.ml_price_predictions (
                price_id,
                loading_date,
                loading_city_gcid,
                unloading_city_gcid,
                body_group,
                dataset_split,
                actual_price_rub,
                predicted_price_rub,
                abs_error_rub,
                ape_pct,
                model_name,
                scored_at
            ) VALUES %s
            """,
            rows,
            page_size=10_000,
        )

    return len(rows)


def main() -> None:
    connection = psycopg2.connect(**get_db_config())

    try:
        dataset = add_time_features(load_dataset(connection))

        train = dataset.loc[dataset["dataset_split"] == "train"].copy()
        test = dataset.loc[dataset["dataset_split"] == "test"].copy()
        scoring = dataset.loc[
            dataset["dataset_split"].isin(["test", "future"])
        ].copy()

        if train.empty or test.empty or scoring.empty:
            raise RuntimeError("train, test and scoring datasets must not be empty")

        train_target = train["actual_price_rub"].astype(float)
        test_target = test["actual_price_rub"].astype(float)

        model = build_model()
        model.fit(
            train[FEATURES],
            train_target,
            cat_features=CATEGORICAL_FEATURES,
        )

        test_predictions = model.predict(test[FEATURES])
        metrics = calculate_metrics(test_target, test_predictions)

        scoring["predicted_price_rub"] = model.predict(scoring[FEATURES])

        model_path = Path(
            os.getenv(
                "ML_MODEL_PATH",
                "/opt/airflow/artifacts/models/catboost_price_model.cbm",
            )
        )
        model_path.parent.mkdir(parents=True, exist_ok=True)
        model.save_model(model_path)

        with connection:
            predictions_loaded = save_predictions(connection, scoring)

        print(
            json.dumps(
                {
                    "model_name": MODEL_NAME,
                    "random_seed": RANDOM_SEED,
                    "train_rows": len(train),
                    "test_rows": len(test),
                    "future_rows": int(
                        (scoring["dataset_split"] == "future").sum()
                    ),
                    "metrics": metrics,
                    "predictions_loaded": predictions_loaded,
                    "model_path": str(model_path),
                },
                ensure_ascii=False,
            )
        )

    finally:
        connection.close()


if __name__ == "__main__":
    main()
