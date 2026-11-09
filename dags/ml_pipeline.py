import os
import sys

import pendulum
import psycopg2
from airflow.sdk import dag, task

sys.path.insert(0, "/opt/airflow")


def get_db_config() -> dict[str, str | int]:
    return {
        "host": os.environ["ETL_DB_HOST"],
        "port": int(os.getenv("ETL_DB_PORT", "5432")),
        "dbname": os.environ["ETL_DB_NAME"],
        "user": os.environ["ETL_DB_USER"],
        "password": os.environ["ETL_DB_PASSWORD"],
    }


@dag(
    dag_id="ml_pipeline",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["ml", "prices"],
)
def ml_pipeline():
    @task
    def train_and_score() -> None:
        from ml.train_and_score import main

        main()

    @task
    def check_predictions() -> dict[str, int]:
        connection = psycopg2.connect(**get_db_config())

        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        COUNT(*) FILTER (
                            WHERE dataset_split = 'test'
                        ) AS test_rows,
                        COUNT(*) FILTER (
                            WHERE dataset_split = 'future'
                        ) AS future_rows,
                        COUNT(*) FILTER (
                            WHERE dataset_split = 'future'
                              AND (
                                  actual_price_rub IS NOT NULL
                                  OR abs_error_rub IS NOT NULL
                                  OR ape_pct IS NOT NULL
                              )
                        ) AS invalid_future_rows,
                        COUNT(*) FILTER (
                            WHERE predicted_price_rub <= 0
                        ) AS non_positive_predictions
                    FROM dm.ml_price_predictions
                    """
                )
                row = cursor.fetchone()
                columns = [description.name for description in cursor.description]
                result = dict(zip(columns, row))

            if result["test_rows"] == 0 or result["future_rows"] == 0:
                raise RuntimeError("test or future predictions are missing")

            if result["invalid_future_rows"] != 0:
                raise RuntimeError("future rows contain actual values or errors")

            if result["non_positive_predictions"] != 0:
                raise RuntimeError("non-positive predictions were found")

            return result

        finally:
            connection.close()

    predictions = train_and_score()
    validation = check_predictions()
    predictions >> validation


ml_pipeline()
