import sys
from datetime import date

import pendulum
from airflow.sdk import dag, get_current_context, task

sys.path.insert(0, "/opt/airflow")

from etl.load_stg import (  # noqa: E402
    load_body_groups_to_dds,
    load_city_dimension_to_dds,
    load_market_corridor_to_dm,
    load_price_months_to_dds,
    load_price_relations_to_stg,
    load_prices_to_ods,
    load_prices_to_stg,
    load_reference_data_to_stg,
    publish_market_corridor_to_clickhouse,
    validate_pipeline,
)


def parse_interval(interval: dict[str, str]) -> tuple[date, date]:
    return (
        date.fromisoformat(interval["start_date"]),
        date.fromisoformat(interval["end_date"]),
    )


@dag(
    dag_id="dwh_pipeline",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["dwh", "prices"],
)
def dwh_pipeline():
    @task
    def resolve_interval() -> dict[str, str]:
        context = get_current_context()
        dag_run = context.get("dag_run")
        conf = dag_run.conf if dag_run and dag_run.conf else {}

        start_date = conf.get("start_date", "2000-01-01")
        end_date = conf.get("end_date", "2100-01-01")

        if date.fromisoformat(start_date) >= date.fromisoformat(end_date):
            raise ValueError("start_date must be earlier than end_date")

        return {
            "start_date": start_date,
            "end_date": end_date,
        }

    @task
    def load_references() -> dict[str, str | int]:
        return load_reference_data_to_stg()

    @task
    def load_prices(interval: dict[str, str]) -> dict[str, str | int]:
        start_date, end_date = parse_interval(interval)
        return load_prices_to_stg(start_date, end_date)

    @task
    def load_price_relations(interval: dict[str, str]) -> dict[str, str | int]:
        start_date, end_date = parse_interval(interval)
        return load_price_relations_to_stg(start_date, end_date)

    @task
    def validate_and_load_ods(
        interval: dict[str, str],
    ) -> dict[str, str | int]:
        start_date, end_date = parse_interval(interval)
        return load_prices_to_ods(start_date, end_date)

    @task
    def load_city_dimension() -> dict[str, str | int]:
        return load_city_dimension_to_dds()

    @task
    def load_body_groups(interval: dict[str, str]) -> dict[str, str | int]:
        start_date, end_date = parse_interval(interval)
        return load_body_groups_to_dds(start_date, end_date)

    @task
    def load_price_months(interval: dict[str, str]) -> dict[str, int]:
        start_date, end_date = parse_interval(interval)
        return load_price_months_to_dds(start_date, end_date)

    @task
    def build_market_corridor() -> dict[str, int]:
        return load_market_corridor_to_dm()

    @task
    def check_result(interval: dict[str, str]) -> dict[str, int]:
        start_date, end_date = parse_interval(interval)
        return validate_pipeline(start_date, end_date)

    @task
    def publish_clickhouse_mart() -> dict[str, int]:
        return publish_market_corridor_to_clickhouse()

    interval = resolve_interval()
    references = load_references()
    prices = load_prices(interval)
    relations = load_price_relations(interval)
    ods = validate_and_load_ods(interval)
    city = load_city_dimension()
    bodies = load_body_groups(interval)
    months = load_price_months(interval)
    mart = build_market_corridor()
    result = check_result(interval)
    clickhouse = publish_clickhouse_mart()

    prices >> relations
    [references, prices] >> ods
    references >> city
    [references, relations, ods] >> bodies
    [relations, ods] >> months
    [ods, city, bodies, months] >> mart
    mart >> result
    result >> clickhouse


dwh_pipeline()
