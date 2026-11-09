import argparse
import os
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import uuid4

import psycopg2


def get_db_config() -> dict[str, str | int]:
    return {
        "host": os.environ["ETL_DB_HOST"],
        "port": int(os.getenv("ETL_DB_PORT", "5432")),
        "dbname": os.environ["ETL_DB_NAME"],
        "user": os.environ["ETL_DB_USER"],
        "password": os.environ["ETL_DB_PASSWORD"],
    }


def get_clickhouse_config() -> dict[str, str | int]:
    return {
        "host": os.environ["CLICKHOUSE_HOST"],
        "port": int(os.getenv("CLICKHOUSE_PORT", "8123")),
        "database": os.environ["CLICKHOUSE_DB"],
        "username": os.environ["CLICKHOUSE_USER"],
        "password": os.environ["CLICKHOUSE_PASSWORD"],
    }


def load_prices_to_stg(
    start_date: date,
    end_date: date,
) -> dict[str, str | int]:
    batch_id = str(uuid4())
    observed_at = datetime.now(timezone.utc)

    connection = psycopg2.connect(**get_db_config())

    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    CREATE TEMP TABLE tmp_prices
                    ON COMMIT DROP
                    AS
                    SELECT
                        p.*,
                        md5(to_jsonb(p)::text) AS _row_hash
                    FROM public.prices AS p
                    WHERE p.loading_date >= %s AND p.loading_date < %s
                    """,
                    (start_date, end_date),
                )

                cursor.execute(
                    """
                    CREATE UNIQUE INDEX tmp_prices_id_uq
                        ON tmp_prices (id)
                    """
                )

                cursor.execute(
                    """
                    UPDATE stg.prices AS target
                    SET _superseded_at = %s
                    FROM tmp_prices AS source
                    WHERE target.id = source.id
                        AND target._superseded_at IS NULL
                        AND target._row_hash <> source._row_hash
                    """,
                    (observed_at,),
                )

                closed_versions = cursor.rowcount

                cursor.execute(
                    """
                    INSERT INTO stg.prices (
                        id,
                        created_at,
                        updated_at,
                        subscriber_id,
                        organization_id,
                        loading_date,
                        loading_city_gcid,
                        unloading_city_gcid,
                        distance,
                        total_weight,
                        total_volume,
                        body_type,
                        price,
                        currency_code,
                        complex,
                        price_type,
                        item_id,
                        is_fact,
                        nearest_rate_date,
                        meta_item_id,
                        end_loading_date,
                        source,
                        external_item_uuid,
                        outlier_mark,
                        weight_category,
                        volume_category,
                        price_group_id,
                        _batch_id,
                        _row_hash,
                        _observed_at
                    )
                    SELECT
                        src.id,
                        src.created_at,
                        src.updated_at,
                        src.subscriber_id,
                        src.organization_id,
                        src.loading_date,
                        src.loading_city_gcid,
                        src.unloading_city_gcid,
                        src.distance,
                        src.total_weight,
                        src.total_volume,
                        src.body_type,
                        src.price,
                        src.currency_code,
                        src.complex,
                        src.price_type,
                        src.item_id,
                        src.is_fact,
                        src.nearest_rate_date,
                        src.meta_item_id,
                        src.end_loading_date,
                        src.source,
                        src.external_item_uuid,
                        src.outlier_mark,
                        src.weight_category,
                        src.volume_category,
                        src.price_group_id,
                        %s,
                        src._row_hash,
                        %s
                    FROM tmp_prices AS src
                    LEFT JOIN stg.prices AS target
                        ON target.id = src.id
                            AND target._superseded_at IS NULL
                    WHERE target.id IS NULL
                    """,
                    (batch_id, observed_at),
                )

                inserted_versions = cursor.rowcount
        return {
            "batch_id": batch_id,
            "closed_versions": closed_versions,
            "inserted_versions": inserted_versions,
        }

    finally:
        connection.close()


def load_reference_data_to_stg() -> dict[str, str | int]:
    batch_id = str(uuid4())
    observed_at = datetime.now(timezone.utc)

    connection = psycopg2.connect(**get_db_config())

    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    TRUNCATE TABLE
                        stg.cities,
                        stg.exchange_rates,
                        stg.body_types
                    RESTART IDENTITY
                    """
                )

                cursor.execute(
                    """
                    INSERT INTO stg.cities (
                        id,
                        full_name_local,
                        full_name_gc,
                        gc_id,
                        name,
                        longitude,
                        latitude,
                        created_at,
                        updated_at,
                        _batch_id,
                        _row_hash,
                        _observed_at
                    )
                    SELECT
                        src.id,
                        src.full_name_local,
                        src.full_name_gc,
                        src.gc_id,
                        src.name,
                        src.longitude,
                        src.latitude,
                        src.created_at,
                        src.updated_at,
                        %s,
                        md5(to_jsonb(src)::text),
                        %s
                    FROM public.cities AS src
                    """,
                    (batch_id, observed_at),
                )

                cities_loaded = cursor.rowcount

                cursor.execute(
                    """
                    INSERT INTO stg.exchange_rates (
                        id,
                        num_code,
                        value,
                        setting_date,
                        _batch_id,
                        _row_hash,
                        _observed_at
                    )
                    SELECT
                        src.id,
                        src.num_code,
                        src.value,
                        src.setting_date,
                        %s,
                        md5(to_jsonb(src)::text),
                        %s
                    FROM public.exchange_rates AS src
                    """,
                    (batch_id, observed_at),
                )

                exchange_rates_loaded = cursor.rowcount

                cursor.execute(
                    """
                    INSERT INTO stg.body_types (
                        id,
                        name,
                        id_from_1c,
                        std_id,
                        _batch_id,
                        _row_hash,
                        _observed_at
                    )
                    SELECT
                        src.id,
                        src.name,
                        src."id_from_1C",
                        src.std_id,
                        %s,
                        md5(to_jsonb(src)::text),
                        %s
                    FROM public.body_types AS src
                    """,
                    (batch_id, observed_at),
                )

                body_types_loaded = cursor.rowcount

        return {
            "batch_id": batch_id,
            "cities_loaded": cities_loaded,
            "exchange_rates_loaded": exchange_rates_loaded,
            "body_types_loaded": body_types_loaded,
        }

    finally:
        connection.close()


def load_price_relations_to_stg(
    start_date: date,
    end_date: date,
) -> dict[str, str | int]:
    batch_id = str(uuid4())
    observed_at = datetime.now(timezone.utc)

    connection = psycopg2.connect(**get_db_config())

    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    DELETE FROM stg.price_body_types AS target
                    USING public.prices AS price
                    WHERE target.price_id = price.id
                      AND price.loading_date >= %s
                      AND price.loading_date < %s
                    """,
                    (start_date, end_date),
                )

                cursor.execute(
                    """
                    INSERT INTO stg.price_body_types (
                        id,
                        price_id,
                        body_type_id,
                        _batch_id,
                        _row_hash,
                        _observed_at
                    )
                    SELECT
                        relation.id,
                        relation.price_id,
                        relation.body_type_id,
                        %s,
                        md5(to_jsonb(relation)::text),
                        %s
                    FROM public.price_body_types AS relation
                    JOIN public.prices AS price
                      ON price.id = relation.price_id
                    WHERE price.loading_date >= %s
                      AND price.loading_date < %s
                    """,
                    (batch_id, observed_at, start_date, end_date),
                )

                price_body_types_loaded = cursor.rowcount

                cursor.execute(
                    """
                    DELETE FROM stg.price_dates AS target
                    USING public.prices AS price
                    WHERE target.price_id = price.id
                      AND price.loading_date >= %s
                      AND price.loading_date < %s
                    """,
                    (start_date, end_date),
                )

                cursor.execute(
                    """
                    INSERT INTO stg.price_dates (
                        id,
                        price_id,
                        price_date,
                        _batch_id,
                        _row_hash,
                        _observed_at
                    )
                    SELECT
                        relation.id,
                        relation.price_id,
                        relation.price_date,
                        %s,
                        md5(to_jsonb(relation)::text),
                        %s
                    FROM public.price_dates AS relation
                    JOIN public.prices AS price
                      ON price.id = relation.price_id
                    WHERE price.loading_date >= %s
                      AND price.loading_date < %s
                    """,
                    (batch_id, observed_at, start_date, end_date),
                )

                price_dates_loaded = cursor.rowcount

        return {
            "batch_id": batch_id,
            "price_body_types_loaded": price_body_types_loaded,
            "price_dates_loaded": price_dates_loaded,
        }

    finally:
        connection.close()


def load_prices_to_ods(
    start_date: date,
    end_date: date,
) -> dict[str, str | int]:
    batch_id = str(uuid4())
    processed_at = datetime.now(timezone.utc)

    connection = psycopg2.connect(**get_db_config())

    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    CREATE TEMP TABLE tmp_validated_prices
                    ON COMMIT DROP
                    AS
                    WITH base AS (
                        SELECT
                            price.*,
                            CASE
                                WHEN price.currency_code = '643'
                                    THEN 1.0
                                ELSE (
                                    SELECT rate.value
                                    FROM stg.exchange_rates AS rate
                                    WHERE rate.num_code = price.currency_code
                                        AND rate.setting_date = price.nearest_rate_date
                                )
                            END AS exchange_rate,
                            EXISTS (
                                SELECT 1
                                FROM stg.cities AS city
                                WHERE city.gc_id = price.loading_city_gcid
                            ) AS loading_city_exists,
                            EXISTS (
                                SELECT 1
                                FROM stg.cities AS city
                                WHERE city.gc_id = price.unloading_city_gcid
                            ) AS unloading_city_exists
                        FROM stg.prices AS price
                        WHERE price._superseded_at IS NULL
                        AND price.loading_date >= %s
                        AND price.loading_date < %s
                    )
                    SELECT
                        base.*,
                        (base.price * base.exchange_rate)::NUMERIC(18, 2)
                            AS price_rub,
                        COALESCE(
                            base.weight_category,
                            CASE
                                WHEN ROUND(base.total_weight::NUMERIC, 1) >= 30
                                    THEN 'G20T'
                                WHEN ROUND(base.total_weight::NUMERIC, 1) >= 10
                                    THEN 'FTL'
                                WHEN ROUND(base.total_weight::NUMERIC, 1) >= 5
                                    THEN '10T'
                                WHEN ROUND(base.total_weight::NUMERIC, 1) >= 2.5
                                    THEN '5T'
                                WHEN ROUND(base.total_weight::NUMERIC, 1) >= 0.5
                                    THEN 'L5T'
                                ELSE 'OTH'
                            END
                        ) AS normalized_weight_category,
                        COALESCE(
                            base.volume_category,
                            CASE
                                WHEN base.total_volume > 0
                                AND base.total_volume <= 120
                                    THEN 'FTL'
                                ELSE 'OTH'
                            END
                        ) AS normalized_volume_category,
                        ARRAY_REMOVE(
                            ARRAY[
                                CASE
                                    WHEN base.price <= 0
                                        THEN 'INVALID_PRICE'
                                END,
                                CASE
                                    WHEN base.currency_code = '643'
                                    AND base.price <= 1000
                                        THEN 'RUB_PRICE_TOO_LOW'
                                END,
                                CASE
                                    WHEN base.distance <= 0
                                        THEN 'INVALID_DISTANCE'
                                END,
                                CASE
                                    WHEN base.total_weight < 0
                                        THEN 'NEGATIVE_WEIGHT'
                                END,
                                CASE
                                    WHEN base.total_volume < 0
                                        THEN 'NEGATIVE_VOLUME'
                                END,
                                CASE
                                    WHEN NOT base.loading_city_exists
                                        THEN 'LOADING_CITY_NOT_FOUND'
                                END,
                                CASE
                                    WHEN NOT base.unloading_city_exists
                                        THEN 'UNLOADING_CITY_NOT_FOUND'
                                END,
                                CASE
                                    WHEN base.currency_code <> '643'
                                    AND base.exchange_rate IS NULL
                                        THEN 'EXCHANGE_RATE_NOT_FOUND'
                                END
                            ]::TEXT[],
                            NULL
                        ) AS reason_codes
                    FROM base
                    """,
                    (start_date, end_date),
                )

                cursor.execute(
                    """
                    CREATE UNIQUE INDEX tmp_validated_prices_id_uq
                        ON tmp_validated_prices (id)
                    """
                )

                cursor.execute(
                    """
                    DELETE FROM ods.dq_quarantine_prices AS quarantine
                    USING tmp_validated_prices AS validated
                    WHERE quarantine.price_id = validated.id
                    AND CARDINALITY(validated.reason_codes) = 0
                    """
                )

                resolved_quarantine = cursor.rowcount

                cursor.execute(
                    """
                    DELETE FROM ods.prices AS ods_price
                    USING tmp_validated_prices AS validated
                    WHERE ods_price.price_id = validated.id
                    AND CARDINALITY(validated.reason_codes) > 0
                    """
                )

                invalidated_prices = cursor.rowcount

                cursor.execute(
                    """
                    INSERT INTO ods.dq_quarantine_prices AS target (
                        price_id,
                        source_version_id,
                        reason_codes,
                        batch_id,
                        quarantined_at
                    )
                    SELECT
                        validated.id,
                        validated._version_id,
                        validated.reason_codes,
                        %s,
                        %s
                    FROM tmp_validated_prices AS validated
                    WHERE CARDINALITY(validated.reason_codes) > 0
                    ON CONFLICT (price_id)
                    DO UPDATE SET
                        source_version_id = EXCLUDED.source_version_id,
                        reason_codes = EXCLUDED.reason_codes,
                        batch_id = EXCLUDED.batch_id,
                        quarantined_at = EXCLUDED.quarantined_at
                    WHERE target.source_version_id
                            IS DISTINCT FROM EXCLUDED.source_version_id
                    OR target.reason_codes
                            IS DISTINCT FROM EXCLUDED.reason_codes
                    """,
                    (batch_id, processed_at),
                )

                quarantined_prices = cursor.rowcount

                cursor.execute(
                    """
                    INSERT INTO ods.prices AS target (
                        price_id,
                        source_version_id,
                        batch_id,
                        processed_at,
                        source_created_at,
                        source_updated_at,
                        subscriber_id,
                        organization_id,
                        loading_date,
                        end_loading_date,
                        loading_city_gcid,
                        unloading_city_gcid,
                        distance,
                        total_weight,
                        total_volume,
                        body_type,
                        price_original,
                        currency_code,
                        exchange_rate,
                        price_rub,
                        nearest_rate_date,
                        complex,
                        price_type,
                        is_fact,
                        outlier_mark,
                        weight_category,
                        volume_category
                    )
                    SELECT
                        validated.id,
                        validated._version_id,
                        %s,
                        %s,
                        validated.created_at,
                        validated.updated_at,
                        validated.subscriber_id,
                        validated.organization_id,
                        validated.loading_date,
                        validated.end_loading_date,
                        validated.loading_city_gcid,
                        validated.unloading_city_gcid,
                        validated.distance,
                        validated.total_weight,
                        validated.total_volume,
                        validated.body_type,
                        validated.price,
                        validated.currency_code,
                        validated.exchange_rate,
                        validated.price_rub,
                        validated.nearest_rate_date,
                        validated.complex,
                        validated.price_type,
                        validated.is_fact,
                        validated.outlier_mark,
                        validated.normalized_weight_category,
                        validated.normalized_volume_category
                    FROM tmp_validated_prices AS validated
                    WHERE CARDINALITY(validated.reason_codes) = 0
                    ON CONFLICT (price_id)
                    DO UPDATE SET
                        source_version_id = EXCLUDED.source_version_id,
                        batch_id = EXCLUDED.batch_id,
                        processed_at = EXCLUDED.processed_at,
                        source_created_at = EXCLUDED.source_created_at,
                        source_updated_at = EXCLUDED.source_updated_at,
                        subscriber_id = EXCLUDED.subscriber_id,
                        organization_id = EXCLUDED.organization_id,
                        loading_date = EXCLUDED.loading_date,
                        end_loading_date = EXCLUDED.end_loading_date,
                        loading_city_gcid = EXCLUDED.loading_city_gcid,
                        unloading_city_gcid = EXCLUDED.unloading_city_gcid,
                        distance = EXCLUDED.distance,
                        total_weight = EXCLUDED.total_weight,
                        total_volume = EXCLUDED.total_volume,
                        body_type = EXCLUDED.body_type,
                        price_original = EXCLUDED.price_original,
                        currency_code = EXCLUDED.currency_code,
                        exchange_rate = EXCLUDED.exchange_rate,
                        price_rub = EXCLUDED.price_rub,
                        nearest_rate_date = EXCLUDED.nearest_rate_date,
                        complex = EXCLUDED.complex,
                        price_type = EXCLUDED.price_type,
                        is_fact = EXCLUDED.is_fact,
                        outlier_mark = EXCLUDED.outlier_mark,
                        weight_category = EXCLUDED.weight_category,
                        volume_category = EXCLUDED.volume_category
                    WHERE target.source_version_id
                            IS DISTINCT FROM EXCLUDED.source_version_id
                    OR target.exchange_rate
                            IS DISTINCT FROM EXCLUDED.exchange_rate
                    OR target.price_rub
                            IS DISTINCT FROM EXCLUDED.price_rub
                    OR target.weight_category
                            IS DISTINCT FROM EXCLUDED.weight_category
                    OR target.volume_category
                            IS DISTINCT FROM EXCLUDED.volume_category
                    """,
                    (batch_id, processed_at),
                )

                loaded_to_ods = cursor.rowcount

        return {
            "batch_id": batch_id,
            "resolved_quarantine": resolved_quarantine,
            "invalidated_prices": invalidated_prices,
            "quarantined_prices": quarantined_prices,
            "loaded_to_ods": loaded_to_ods,
        }

    finally:
        connection.close()


def load_city_dimension_to_dds() -> dict[str, str | int]:
    batch_id = str(uuid4())
    processed_at = datetime.now(timezone.utc)

    connection = psycopg2.connect(**get_db_config())

    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    TRUNCATE TABLE dds.dim_city
                    """
                )

                cursor.execute(
                    """
                    WITH canonical AS (
                        SELECT DISTINCT ON (city.gc_id)
                            city.id,
                            city.gc_id,
                            city.full_name_local,
                            city.full_name_gc,
                            city.longitude,
                            city.latitude
                        FROM stg.cities AS city
                        WHERE city._superseded_at IS NULL
                        ORDER BY city.gc_id, city.id
                    )
                    INSERT INTO dds.dim_city (
                        gc_id,
                        source_city_id,
                        city_name,
                        region_name,
                        full_name_local,
                        full_name_gc,
                        longitude,
                        latitude,
                        batch_id,
                        processed_at
                    )
                    SELECT
                        canonical.gc_id,
                        canonical.id,
                        BTRIM(SPLIT_PART(canonical.full_name_local, ',', 1)),
                        CASE
                            WHEN POSITION(',' IN canonical.full_name_local) > 0
                                THEN NULLIF(
                                    BTRIM(
                                        SUBSTRING(
                                            canonical.full_name_local
                                            FROM POSITION(',' IN canonical.full_name_local) + 1
                                        )
                                    ),
                                    ''
                                )
                            ELSE NULL
                        END,
                        canonical.full_name_local,
                        canonical.full_name_gc,
                        canonical.longitude,
                        canonical.latitude,
                        %s,
                        %s
                    FROM canonical
                    """,
                    (batch_id, processed_at),
                )

                cities_loaded = cursor.rowcount

        return {
            "batch_id": batch_id,
            "cities_loaded": cities_loaded,
        }

    finally:
        connection.close()


def load_body_groups_to_dds(
    start_date: date,
    end_date: date,
) -> dict[str, str | int]:
    batch_id = str(uuid4())
    processed_at = datetime.now(timezone.utc)

    connection = psycopg2.connect(**get_db_config())

    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    TRUNCATE TABLE dds.dim_body_group
                    """
                )

                cursor.execute(
                    """
                    INSERT INTO dds.dim_body_group (
                        body_group_id,
                        body_group_name,
                        sort_order,
                        batch_id,
                        processed_at
                    )
                    VALUES
                        (56, 'Тент', 1, %s, %s),
                        (4, 'Рефрижератор', 2, %s, %s),
                        (2, 'Изотерм', 3, %s, %s),
                        (15, 'Борт', 4, %s, %s),
                        (0, 'Прочие', 5, %s, %s),
                        (-1, 'Не указан', 6, %s, %s)
                    """,
                    (
                        batch_id,
                        processed_at,
                        batch_id,
                        processed_at,
                        batch_id,
                        processed_at,
                        batch_id,
                        processed_at,
                        batch_id,
                        processed_at,
                        batch_id,
                        processed_at,
                    ),
                )

                body_groups_loaded = cursor.rowcount

                cursor.execute(
                    """
                    DELETE FROM dds.bridge_price_body_group AS bridge
                    USING stg.prices AS price
                    WHERE bridge.price_id = price.id
                      AND price._superseded_at IS NULL
                      AND price.loading_date >= %s
                      AND price.loading_date < %s
                    """,
                    (start_date, end_date),
                )

                cursor.execute(
                    """
                    WITH relation_candidates AS (
                        SELECT
                            price.price_id,
                            relation.body_type_id
                        FROM ods.prices AS price
                        JOIN stg.price_body_types AS relation
                          ON relation.price_id = price.price_id
                         AND relation._superseded_at IS NULL
                        WHERE price.loading_date >= %s
                          AND price.loading_date < %s
                    ),
                    fallback_candidates AS (
                        SELECT
                            price.price_id,
                            price.body_type AS body_type_id
                        FROM ods.prices AS price
                        WHERE price.loading_date >= %s
                          AND price.loading_date < %s
                          AND NOT EXISTS (
                              SELECT 1
                              FROM stg.price_body_types AS relation
                              WHERE relation.price_id = price.price_id
                                AND relation._superseded_at IS NULL
                          )
                    ),
                    candidates AS (
                        SELECT * FROM relation_candidates
                        UNION ALL
                        SELECT * FROM fallback_candidates
                    ),
                    mapped AS (
                        SELECT
                            candidate.price_id,
                            CASE
                                WHEN candidate.body_type_id IS NULL
                                    THEN -1
                                WHEN body_type.std_id IN (56, 4, 2, 15)
                                    THEN body_type.std_id
                                ELSE 0
                            END AS body_group_id
                        FROM candidates AS candidate
                        LEFT JOIN stg.body_types AS body_type
                          ON body_type.id = candidate.body_type_id
                         AND body_type._superseded_at IS NULL
                    )
                    INSERT INTO dds.bridge_price_body_group (
                        price_id,
                        body_group_id,
                        batch_id,
                        processed_at
                    )
                    SELECT DISTINCT
                        mapped.price_id,
                        mapped.body_group_id,
                        %s::uuid,
                        %s
                    FROM mapped
                    """,
                    (
                        start_date,
                        end_date,
                        start_date,
                        end_date,
                        batch_id,
                        processed_at,
                    ),
                )

                price_body_groups_loaded = cursor.rowcount

        return {
            "batch_id": batch_id,
            "body_groups_loaded": body_groups_loaded,
            "price_body_groups_loaded": price_body_groups_loaded,
        }

    finally:
        connection.close()


def load_price_months_to_dds(
    start_date: date,
    end_date: date,
) -> dict[str, int]:
    connection = psycopg2.connect(**get_db_config())

    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    DELETE FROM dds.bridge_price_month AS bridge
                    USING stg.prices AS price
                    WHERE bridge.price_id = price.id
                      AND price._superseded_at IS NULL
                      AND price.loading_date >= %s
                      AND price.loading_date < %s
                    """,
                    (start_date, end_date),
                )

                cursor.execute(
                    """
                    INSERT INTO dds.bridge_price_month (
                        price_id,
                        loading_month
                    )
                    SELECT DISTINCT
                        price.price_id,
                        DATE_TRUNC('month', price_date.price_date)::date
                    FROM ods.prices AS price
                    JOIN stg.price_dates AS price_date
                      ON price_date.price_id = price.price_id
                     AND price_date._superseded_at IS NULL
                    WHERE price.loading_date >= %s
                      AND price.loading_date < %s
                    """,
                    (start_date, end_date),
                )

                price_months_loaded = cursor.rowcount

        return {
            "price_months_loaded": price_months_loaded,
        }

    finally:
        connection.close()


def load_market_corridor_to_dm() -> dict[str, int]:
    sql_path = (
        Path(__file__).resolve().parents[1]
        / "sql"
        / "dml"
        / "mart_market_corridor.sql"
    )
    query = sql_path.read_text(encoding="utf-8")

    connection = psycopg2.connect(**get_db_config())

    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                mart_rows_loaded = cursor.rowcount

        return {
            "mart_rows_loaded": mart_rows_loaded,
        }

    finally:
        connection.close()


def publish_market_corridor_to_clickhouse(
    batch_size: int = 10_000,
) -> dict[str, int]:
    import clickhouse_connect

    columns = (
        "loading_month",
        "loading_city_gcid",
        "loading_city_name",
        "unloading_city_gcid",
        "unloading_city_name",
        "body_group_id",
        "body_group_name",
        "weight_category",
        "p25_trip",
        "median_trip",
        "p75_trip",
        "avg_trip",
        "p25_km",
        "median_km",
        "p75_km",
        "trips_count",
        "carriers_count",
        "cv_rate",
        "mom_pct",
        "yoy_pct",
        "reliability_level",
    )
    grain = (
        "loading_month",
        "loading_city_gcid",
        "unloading_city_gcid",
        "body_group_id",
        "weight_category",
    )
    ddl_path = (
        Path(__file__).resolve().parents[1]
        / "sql"
        / "ddl"
        / "clickhouse_mart.sql"
    )

    postgres = psycopg2.connect(**get_db_config())
    clickhouse = clickhouse_connect.get_client(
        **get_clickhouse_config(),
    )

    def table_stats(table: str) -> tuple[int, int, int]:
        query = f"""
            SELECT
                count(),
                uniqExact(tuple({', '.join(grain)})),
                toUInt64(sum(trips_count))
            FROM {table}
        """
        row = clickhouse.query(query).result_rows[0]
        return tuple(int(value) for value in row)

    def active_partitions(table: str) -> set[str]:
        query = f"""
            SELECT DISTINCT partition
            FROM system.parts
            WHERE database = currentDatabase()
              AND table = '{table}'
              AND active
        """
        return {
            str(row[0])
            for row in clickhouse.query(query).result_rows
        }

    try:
        ddl = ddl_path.read_text(encoding="utf-8")
        for statement in ddl.split(";"):
            if statement.strip():
                clickhouse.command(statement)

        clickhouse.command("TRUNCATE TABLE market_corridor_stage")

        with postgres.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    COUNT(*),
                    COALESCE(SUM(trips_count), 0)
                FROM dm.mart_market_corridor
                """
            )
            source_rows, source_trips = cursor.fetchone()

        loaded_rows = 0
        with postgres.cursor(name="clickhouse_market_corridor") as cursor:
            cursor.itersize = batch_size
            cursor.execute(
                f"""
                SELECT {', '.join(columns)}
                FROM dm.mart_market_corridor
                ORDER BY {', '.join(grain)}
                """
            )

            while rows := cursor.fetchmany(batch_size):
                clickhouse.insert(
                    "market_corridor_stage",
                    rows,
                    column_names=columns,
                )
                loaded_rows += len(rows)

        stage_rows, stage_unique_rows, stage_trips = table_stats(
            "market_corridor_stage",
        )
        if loaded_rows != source_rows or stage_rows != source_rows:
            raise RuntimeError(
                "PostgreSQL and ClickHouse staging row counts do not match"
            )
        if stage_unique_rows != stage_rows:
            raise RuntimeError("duplicate ClickHouse mart grain was found")
        if stage_trips != int(source_trips):
            raise RuntimeError(
                "PostgreSQL and ClickHouse staging trip checksums do not match"
            )

        stage_partitions = active_partitions("market_corridor_stage")
        target_partitions = active_partitions("market_corridor")

        for partition in sorted(stage_partitions):
            if not partition.isdigit():
                raise RuntimeError(
                    f"unexpected ClickHouse partition: {partition}"
                )
            clickhouse.command(
                "ALTER TABLE market_corridor "
                f"REPLACE PARTITION {partition} "
                "FROM market_corridor_stage"
            )

        for partition in sorted(target_partitions - stage_partitions):
            if not partition.isdigit():
                raise RuntimeError(
                    f"unexpected ClickHouse partition: {partition}"
                )
            clickhouse.command(
                "ALTER TABLE market_corridor "
                f"DROP PARTITION {partition}"
            )

        target_rows, target_unique_rows, target_trips = table_stats(
            "market_corridor",
        )
        if (
            target_rows != stage_rows
            or target_unique_rows != stage_unique_rows
            or target_trips != stage_trips
        ):
            raise RuntimeError(
                "ClickHouse target validation failed after partition replacement"
            )

        clickhouse.command("TRUNCATE TABLE market_corridor_stage")

        return {
            "source_rows": int(source_rows),
            "loaded_rows": loaded_rows,
            "target_rows": target_rows,
            "trips_checksum": target_trips,
            "partitions_replaced": len(stage_partitions),
        }

    finally:
        postgres.close()
        clickhouse.close()


def validate_pipeline(
    start_date: date,
    end_date: date,
) -> dict[str, int]:
    connection = psycopg2.connect(**get_db_config())

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    (
                        SELECT COUNT(*)
                        FROM public.prices
                        WHERE loading_date >= %s
                          AND loading_date < %s
                    ) AS source_prices,
                    (
                        SELECT COUNT(*)
                        FROM stg.prices
                        WHERE _superseded_at IS NULL
                          AND loading_date >= %s
                          AND loading_date < %s
                    ) AS stg_prices,
                    (
                        SELECT COUNT(*)
                        FROM ods.prices
                        WHERE loading_date >= %s
                          AND loading_date < %s
                    ) AS ods_prices,
                    (
                        SELECT COUNT(*)
                        FROM ods.dq_quarantine_prices AS quarantine
                        JOIN stg.prices AS price
                          ON price._version_id = quarantine.source_version_id
                        WHERE price.loading_date >= %s
                          AND price.loading_date < %s
                    ) AS quarantined_prices,
                    (
                        SELECT COUNT(*)
                        FROM ods.prices AS price
                        JOIN ods.dq_quarantine_prices AS quarantine
                          ON quarantine.price_id = price.price_id
                        WHERE price.loading_date >= %s
                          AND price.loading_date < %s
                    ) AS ods_quarantine_overlap,
                    (
                        SELECT COUNT(*)
                        FROM ods.prices AS price
                        WHERE price.loading_date >= %s
                          AND price.loading_date < %s
                          AND NOT EXISTS (
                              SELECT 1
                              FROM dds.bridge_price_body_group AS body
                              WHERE body.price_id = price.price_id
                          )
                    ) AS prices_without_body_group,
                    (
                        SELECT COUNT(*)
                        FROM ods.prices AS price
                        WHERE price.loading_date >= %s
                          AND price.loading_date < %s
                          AND NOT EXISTS (
                              SELECT 1
                              FROM dds.bridge_price_month AS month
                              WHERE month.price_id = price.price_id
                          )
                    ) AS prices_without_month,
                    (
                        SELECT COUNT(*)
                        FROM dm.mart_market_corridor
                    ) AS mart_rows
                """,
                (
                    start_date,
                    end_date,
                    start_date,
                    end_date,
                    start_date,
                    end_date,
                    start_date,
                    end_date,
                    start_date,
                    end_date,
                    start_date,
                    end_date,
                    start_date,
                    end_date,
                ),
            )
            row = cursor.fetchone()
            columns = [description.name for description in cursor.description]
            result = dict(zip(columns, row))

        if result["source_prices"] != result["stg_prices"]:
            raise RuntimeError("source and current STG counts do not match")

        if result["stg_prices"] != (
            result["ods_prices"] + result["quarantined_prices"]
        ):
            raise RuntimeError("STG rows are not fully distributed to ODS/quarantine")

        if result["ods_quarantine_overlap"] != 0:
            raise RuntimeError("a price exists in ODS and quarantine simultaneously")

        if result["prices_without_body_group"] != 0:
            raise RuntimeError("valid prices without a body group were found")

        if result["prices_without_month"] != 0:
            raise RuntimeError("valid prices without a price month were found")

        as_of = date(2026, 10, 26)
        interval_contains_history = start_date <= as_of

        if (
            interval_contains_history
            and result["source_prices"] > 0
            and result["mart_rows"] == 0
        ):
            raise RuntimeError("market corridor mart is empty")

        return result

    finally:
        connection.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load and validate prices for the selected date interval",
    )
    parser.add_argument(
        "start_date",
        type=date.fromisoformat,
        help="Inclusive interval start in YYYY-MM-DD format",
    )
    parser.add_argument(
        "end_date",
        type=date.fromisoformat,
        help="Exclusive interval end in YYYY-MM-DD format",
    )

    args = parser.parse_args()

    if args.start_date >= args.end_date:
        parser.error("start_date must be earlier than end_date")

    reference_result = load_reference_data_to_stg()

    prices_result = load_prices_to_stg(
        start_date=args.start_date,
        end_date=args.end_date,
    )

    relations_result = load_price_relations_to_stg(
        start_date=args.start_date,
        end_date=args.end_date,
    )

    ods_result = load_prices_to_ods(
        start_date=args.start_date,
        end_date=args.end_date,
    )

    city_dimension_result = load_city_dimension_to_dds()

    body_groups_result = load_body_groups_to_dds(
        start_date=args.start_date,
        end_date=args.end_date,
    )

    price_months_result = load_price_months_to_dds(
        start_date=args.start_date,
        end_date=args.end_date,
    )

    market_corridor_result = load_market_corridor_to_dm()

    validation_result = validate_pipeline(
        start_date=args.start_date,
        end_date=args.end_date,
    )

    clickhouse_result = publish_market_corridor_to_clickhouse()

    print(
        {
            "reference_data": reference_result,
            "prices": prices_result,
            "price_relations": relations_result,
            "ods": ods_result,
            "city_dimension": city_dimension_result,
            "body_groups": body_groups_result,
            "price_months": price_months_result,
            "market_corridor": market_corridor_result,
            "validation": validation_result,
            "clickhouse": clickhouse_result,
        }
    )


if __name__ == "__main__":
    main()
