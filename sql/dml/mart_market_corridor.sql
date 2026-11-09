ANALYZE stg.price_dates;
ANALYZE ods.prices;
ANALYZE dds.bridge_price_body_group;
ANALYZE dds.bridge_price_month;

CREATE TEMP TABLE market_corridor_aggregated AS
WITH params AS (
    SELECT DATE '2026-10-26' AS as_of
),
market_base AS (
    SELECT
        price.price_id,
        price_month.loading_month,
        price.loading_city_gcid,
        loading_city.city_name AS loading_city_name,
        price.unloading_city_gcid,
        unloading_city.city_name AS unloading_city_name,
        body_group.body_group_id,
        body_group.body_group_name,
        price.weight_category,
        price.organization_id,
        price.price_type,
        price.price_rub,
        price.distance,
        price.price_rub / NULLIF(price.distance, 0) AS price_per_km
    FROM ods.prices AS price
    JOIN dds.bridge_price_month AS price_month
      ON price_month.price_id = price.price_id
    JOIN dds.bridge_price_body_group AS price_body_group
      ON price_body_group.price_id = price.price_id
    JOIN dds.dim_body_group AS body_group
      ON body_group.body_group_id = price_body_group.body_group_id
    JOIN dds.dim_city AS loading_city
      ON loading_city.gc_id = price.loading_city_gcid
    JOIN dds.dim_city AS unloading_city
      ON unloading_city.gc_id = price.unloading_city_gcid
    CROSS JOIN params
    WHERE price.is_fact IS TRUE
      AND price.complex IS FALSE
      AND price.price_rub > 0
      AND price.distance > 0
      AND price.outlier_mark IS FALSE
      AND EXISTS (
          SELECT 1
          FROM stg.price_dates AS source_date
          WHERE source_date.price_id = price.price_id
            AND source_date._superseded_at IS NULL
            AND DATE_TRUNC('month', source_date.price_date)::date
                = price_month.loading_month
            AND source_date.price_date <= params.as_of
      )
)
SELECT
    loading_month,
    loading_city_gcid,
    loading_city_name,
    unloading_city_gcid,
    unloading_city_name,
    body_group_id,
    body_group_name,
    weight_category,
    ROUND(
        PERCENTILE_CONT(0.25)
            WITHIN GROUP (ORDER BY price_rub)::numeric,
        2
    ) AS p25_trip,
    ROUND(
        PERCENTILE_CONT(0.50)
            WITHIN GROUP (ORDER BY price_rub)::numeric,
        2
    ) AS median_trip,
    ROUND(
        PERCENTILE_CONT(0.75)
            WITHIN GROUP (ORDER BY price_rub)::numeric,
        2
    ) AS p75_trip,
    ROUND(AVG(price_rub), 2) AS avg_trip,
    ROUND(
        PERCENTILE_CONT(0.25)
            WITHIN GROUP (ORDER BY price_per_km)::numeric,
        4
    ) AS p25_km,
    ROUND(
        PERCENTILE_CONT(0.50)
            WITHIN GROUP (ORDER BY price_per_km)::numeric,
        4
    ) AS median_km,
    ROUND(
        PERCENTILE_CONT(0.75)
            WITHIN GROUP (ORDER BY price_per_km)::numeric,
        4
    ) AS p75_km,
    COUNT(DISTINCT price_id) AS trips_count,
    COUNT(DISTINCT organization_id)
        FILTER (WHERE price_type IN (3, 9)) AS carriers_count,
    CASE
        WHEN COUNT(*) < 2 OR AVG(price_rub) = 0 THEN NULL
        ELSE ROUND(
            STDDEV_SAMP(price_rub) / AVG(price_rub),
            6
        )
    END AS cv_rate
FROM market_base
GROUP BY
    loading_month,
    loading_city_gcid,
    loading_city_name,
    unloading_city_gcid,
    unloading_city_name,
    body_group_id,
    body_group_name,
    weight_category;

CREATE UNIQUE INDEX market_corridor_aggregated_grain_idx
ON market_corridor_aggregated (
    loading_month,
    loading_city_gcid,
    unloading_city_gcid,
    body_group_id,
    weight_category
);

ANALYZE market_corridor_aggregated;

CREATE TEMP TABLE market_corridor_publish AS
WITH params AS (
    SELECT DATE '2026-10-26' AS as_of
)
SELECT
    current_row.*,
    CASE
        WHEN previous_month.median_trip IS NULL
          OR previous_month.median_trip = 0
            THEN NULL
        ELSE ROUND(
            (
                current_row.median_trip
                / previous_month.median_trip
                - 1
            ) * 100,
            4
        )
    END AS mom_pct,
    CASE
        WHEN previous_year.median_trip IS NULL
          OR previous_year.median_trip = 0
            THEN NULL
        ELSE ROUND(
            (
                current_row.median_trip
                / previous_year.median_trip
                - 1
            ) * 100,
            4
        )
    END AS yoy_pct,
    CASE
        WHEN current_row.trips_count >= 30
         AND current_row.carriers_count >= 5
         AND params.as_of - current_row.loading_month BETWEEN 0 AND 30
            THEN 'HIGH'
        WHEN current_row.trips_count >= 10
            THEN 'MEDIUM'
        ELSE 'LOW_DATA'
    END AS reliability_level
FROM market_corridor_aggregated AS current_row
LEFT JOIN market_corridor_aggregated AS previous_month
  ON previous_month.loading_month
        = (current_row.loading_month - INTERVAL '1 month')::date
 AND previous_month.loading_city_gcid = current_row.loading_city_gcid
 AND previous_month.unloading_city_gcid = current_row.unloading_city_gcid
 AND previous_month.body_group_id = current_row.body_group_id
 AND previous_month.weight_category = current_row.weight_category
LEFT JOIN market_corridor_aggregated AS previous_year
  ON previous_year.loading_month
        = (current_row.loading_month - INTERVAL '1 year')::date
 AND previous_year.loading_city_gcid = current_row.loading_city_gcid
 AND previous_year.unloading_city_gcid = current_row.unloading_city_gcid
 AND previous_year.body_group_id = current_row.body_group_id
 AND previous_year.weight_category = current_row.weight_category
CROSS JOIN params;

TRUNCATE TABLE dm.mart_market_corridor;

INSERT INTO dm.mart_market_corridor (
    loading_month,
    loading_city_gcid,
    loading_city_name,
    unloading_city_gcid,
    unloading_city_name,
    body_group_id,
    body_group_name,
    weight_category,
    p25_trip,
    median_trip,
    p75_trip,
    avg_trip,
    p25_km,
    median_km,
    p75_km,
    trips_count,
    carriers_count,
    cv_rate,
    mom_pct,
    yoy_pct,
    reliability_level
)
SELECT
    loading_month,
    loading_city_gcid,
    loading_city_name,
    unloading_city_gcid,
    unloading_city_name,
    body_group_id,
    body_group_name,
    weight_category,
    p25_trip,
    median_trip,
    p75_trip,
    avg_trip,
    p25_km,
    median_km,
    p75_km,
    trips_count,
    carriers_count,
    cv_rate,
    mom_pct,
    yoy_pct,
    reliability_level
FROM market_corridor_publish;
