WITH params AS (
    SELECT
        DATE '2026-07-26' AS train_start,
        DATE '2026-10-05' AS test_start,
        DATE '2026-10-26' AS as_of
),
body_groups AS (
    SELECT
        bridge.price_id,
        STRING_AGG(
            body_group.body_group_name,
            '|'
            ORDER BY body_group.sort_order
        ) AS body_group
    FROM dds.bridge_price_body_group AS bridge
    JOIN dds.dim_body_group AS body_group
      ON body_group.body_group_id = bridge.body_group_id
    GROUP BY bridge.price_id
),
candidates AS (
    SELECT
        price.price_id,
        price.loading_date,
        price.loading_city_gcid,
        price.unloading_city_gcid,
        loading_city.city_name AS loading_city_name,
        COALESCE(loading_city.region_name, 'UNKNOWN') AS loading_region,
        loading_city.latitude AS loading_latitude,
        loading_city.longitude AS loading_longitude,
        unloading_city.city_name AS unloading_city_name,
        COALESCE(unloading_city.region_name, 'UNKNOWN') AS unloading_region,
        unloading_city.latitude AS unloading_latitude,
        unloading_city.longitude AS unloading_longitude,
        price.distance,
        price.total_weight,
        price.total_volume,
        body_groups.body_group,
        price.weight_category,
        price.volume_category,
        price.price_rub AS source_price_rub,
        CASE
            WHEN price.loading_date < params.test_start THEN 'train'
            WHEN price.loading_date <= params.as_of THEN 'test'
            ELSE 'future'
        END AS dataset_split
    FROM ods.prices AS price
    JOIN dds.dim_city AS loading_city
      ON loading_city.gc_id = price.loading_city_gcid
    JOIN dds.dim_city AS unloading_city
      ON unloading_city.gc_id = price.unloading_city_gcid
    JOIN body_groups
      ON body_groups.price_id = price.price_id
    CROSS JOIN params
    WHERE price.complex IS FALSE
      AND (
          (
              price.is_fact IS TRUE
              AND price.outlier_mark IS FALSE
              AND price.loading_date >= params.train_start
              AND price.loading_date <= params.as_of
          )
          OR price.loading_date > params.as_of
      )
)
SELECT
    price_id,
    loading_date,
    loading_city_gcid,
    unloading_city_gcid,
    loading_city_name,
    loading_region,
    loading_latitude,
    loading_longitude,
    unloading_city_name,
    unloading_region,
    unloading_latitude,
    unloading_longitude,
    distance,
    total_weight,
    total_volume,
    body_group,
    weight_category,
    volume_category,
    CASE
        WHEN dataset_split = 'future' THEN NULL
        ELSE source_price_rub
    END AS actual_price_rub,
    dataset_split
FROM candidates;
