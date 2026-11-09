CREATE TABLE IF NOT EXISTS analytics.market_corridor
(
    loading_month Date,
    loading_city_gcid Int32,
    loading_city_name LowCardinality(String),
    unloading_city_gcid Int32,
    unloading_city_name LowCardinality(String),
    body_group_id Int32,
    body_group_name LowCardinality(String),
    weight_category LowCardinality(String),
    p25_trip Decimal(18, 2),
    median_trip Decimal(18, 2),
    p75_trip Decimal(18, 2),
    avg_trip Decimal(18, 2),
    p25_km Decimal(18, 4),
    median_km Decimal(18, 4),
    p75_km Decimal(18, 4),
    trips_count UInt64,
    carriers_count UInt64,
    cv_rate Nullable(Decimal(18, 6)),
    mom_pct Nullable(Decimal(12, 4)),
    yoy_pct Nullable(Decimal(12, 4)),
    reliability_level LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(loading_month)
ORDER BY (
    loading_month,
    loading_city_gcid,
    unloading_city_gcid,
    body_group_id,
    weight_category
);

CREATE TABLE IF NOT EXISTS analytics.market_corridor_stage
(
    loading_month Date,
    loading_city_gcid Int32,
    loading_city_name LowCardinality(String),
    unloading_city_gcid Int32,
    unloading_city_name LowCardinality(String),
    body_group_id Int32,
    body_group_name LowCardinality(String),
    weight_category LowCardinality(String),
    p25_trip Decimal(18, 2),
    median_trip Decimal(18, 2),
    p75_trip Decimal(18, 2),
    avg_trip Decimal(18, 2),
    p25_km Decimal(18, 4),
    median_km Decimal(18, 4),
    p75_km Decimal(18, 4),
    trips_count UInt64,
    carriers_count UInt64,
    cv_rate Nullable(Decimal(18, 6)),
    mom_pct Nullable(Decimal(12, 4)),
    yoy_pct Nullable(Decimal(12, 4)),
    reliability_level LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(loading_month)
ORDER BY (
    loading_month,
    loading_city_gcid,
    unloading_city_gcid,
    body_group_id,
    weight_category
);
