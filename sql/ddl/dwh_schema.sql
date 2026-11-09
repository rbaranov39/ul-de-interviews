CREATE SCHEMA IF NOT EXISTS stg;

CREATE SCHEMA IF NOT EXISTS ods;

CREATE SCHEMA IF NOT EXISTS dds;

CREATE SCHEMA IF NOT EXISTS dm;

CREATE TABLE IF NOT EXISTS stg.prices
(
    id BIGINT NOT NULL,
    created_at TIMESTAMP(0),
    updated_at TIMESTAMP(0),
    subscriber_id INT,
    organization_id INT,
    loading_date DATE,
    loading_city_gcid INT,
    unloading_city_gcid INT,
    distance DOUBLE PRECISION,
    total_weight DOUBLE PRECISION,
    total_volume DOUBLE PRECISION,
    body_type INT,
    price DOUBLE PRECISION,
    currency_code VARCHAR(3),
    complex BOOLEAN,
    price_type SMALLINT,
    item_id INT,
    is_fact BOOLEAN,
    nearest_rate_date DATE,
    meta_item_id BIGINT,
    end_loading_date DATE,
    source VARCHAR(255),
    external_item_uuid UUID,
    outlier_mark BOOLEAN,
    weight_category VARCHAR(4),
    volume_category VARCHAR(4),
    price_group_id BIGINT,
    _version_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    _batch_id UUID NOT NULL,
    _row_hash TEXT NOT NULL,
    _observed_at TIMESTAMPTZ NOT NULL,
    _superseded_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS stg_prices_current_id_uq
    ON stg.prices (id)
    WHERE _superseded_at IS NULL;

CREATE TABLE IF NOT EXISTS stg.cities
(
    id BIGINT NOT NULL,
    full_name_local VARCHAR(255),
    full_name_gc VARCHAR(255),
    gc_id INT,
    name VARCHAR(255),
    longitude DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    created_at TIMESTAMP(0),
    updated_at TIMESTAMP(0),
    _version_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    _batch_id UUID NOT NULL,
    _row_hash TEXT NOT NULL,
    _observed_at TIMESTAMPTZ NOT NULL,
    _superseded_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS stg_cities_current_id_uq
    ON stg.cities (id)
    WHERE _superseded_at IS NULL;

CREATE TABLE IF NOT EXISTS stg.exchange_rates
(
    id BIGINT NOT NULL,
    num_code VARCHAR(255),
    value DOUBLE PRECISION,
    setting_date DATE,
    _version_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    _batch_id UUID NOT NULL,
    _row_hash TEXT NOT NULL,
    _observed_at TIMESTAMPTZ NOT NULL,
    _superseded_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS stg_exchange_rates_current_id_uq
    ON stg.exchange_rates (id)
    WHERE _superseded_at IS NULL;

CREATE TABLE IF NOT EXISTS stg.body_types
(
    id BIGINT NOT NULL,
    name VARCHAR(255),
    id_from_1c INT,
    std_id INT,
    _version_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    _batch_id UUID NOT NULL,
    _row_hash TEXT NOT NULL,
    _observed_at TIMESTAMPTZ NOT NULL,
    _superseded_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS stg_body_types_current_id_uq
    ON stg.body_types (id)
    WHERE _superseded_at IS NULL;

CREATE TABLE IF NOT EXISTS stg.price_body_types
(
    id BIGINT NOT NULL,
    price_id BIGINT,
    body_type_id INT,
    _version_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    _batch_id UUID NOT NULL,
    _row_hash TEXT NOT NULL,
    _observed_at TIMESTAMPTZ NOT NULL,
    _superseded_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS stg_price_body_types_current_id_uq
    ON stg.price_body_types (id)
    WHERE _superseded_at IS NULL;

CREATE TABLE IF NOT EXISTS stg.price_dates
(
    id BIGINT NOT NULL,
    price_id BIGINT,
    price_date DATE,
    _version_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    _batch_id UUID NOT NULL,
    _row_hash TEXT NOT NULL,
    _observed_at TIMESTAMPTZ NOT NULL,
    _superseded_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS stg_price_dates_current_id_uq
    ON stg.price_dates (id)
    WHERE _superseded_at IS NULL;

CREATE INDEX IF NOT EXISTS stg_price_dates_price_date_idx
    ON stg.price_dates (price_id, price_date)
    WHERE _superseded_at IS NULL;

CREATE TABLE IF NOT EXISTS ods.dq_quarantine_prices
(
    price_id BIGINT PRIMARY KEY,
    source_version_id BIGINT NOT NULL,
    reason_codes TEXT[] NOT NULL,
    batch_id UUID NOT NULL,
    quarantined_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS ods.prices
(
    price_id BIGINT PRIMARY KEY,
    source_version_id BIGINT NOT NULL,
    batch_id UUID NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL,
    source_created_at TIMESTAMP(0),
    source_updated_at TIMESTAMP(0),
    subscriber_id INTEGER,
    organization_id INTEGER,
    loading_date DATE NOT NULL,
    end_loading_date DATE,
    loading_city_gcid INTEGER NOT NULL,
    unloading_city_gcid INTEGER NOT NULL,
    distance DOUBLE PRECISION NOT NULL,
    total_weight DOUBLE PRECISION NOT NULL,
    total_volume DOUBLE PRECISION NOT NULL,
    body_type INTEGER,
    price_original DOUBLE PRECISION NOT NULL,
    currency_code VARCHAR(3) NOT NULL,
    exchange_rate DOUBLE PRECISION NOT NULL,
    price_rub NUMERIC(18, 2) NOT NULL,
    nearest_rate_date DATE,
    complex BOOLEAN NOT NULL,
    price_type SMALLINT NOT NULL,
    is_fact BOOLEAN NOT NULL,
    outlier_mark BOOLEAN NOT NULL,
    weight_category VARCHAR(4) NOT NULL,
    volume_category VARCHAR(4) NOT NULL
);

CREATE INDEX IF NOT EXISTS ods_prices_loading_date_idx
    ON ods.prices (loading_date);

CREATE TABLE IF NOT EXISTS dds.dim_city
(
    gc_id INTEGER PRIMARY KEY,
    source_city_id BIGINT NOT NULL,
    city_name VARCHAR(255) NOT NULL,
    region_name VARCHAR(255),
    full_name_local VARCHAR(255) NOT NULL,
    full_name_gc VARCHAR(255) NOT NULL,
    longitude DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    batch_id UUID NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.dim_body_group
(
    body_group_id INTEGER PRIMARY KEY,
    body_group_name VARCHAR(64) NOT NULL UNIQUE,
    sort_order SMALLINT NOT NULL,
    batch_id UUID NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.bridge_price_body_group
(
    price_id BIGINT NOT NULL,
    body_group_id INTEGER NOT NULL,
    batch_id UUID NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (price_id, body_group_id)
);

CREATE INDEX IF NOT EXISTS dds_bridge_price_body_group_group_idx
    ON dds.bridge_price_body_group (body_group_id);

CREATE TABLE IF NOT EXISTS dds.bridge_price_month
(
    price_id BIGINT NOT NULL,
    loading_month DATE NOT NULL,
    PRIMARY KEY (price_id, loading_month)
);

CREATE TABLE IF NOT EXISTS dm.mart_market_corridor
(
    loading_month DATE NOT NULL,
    loading_city_gcid INTEGER NOT NULL,
    loading_city_name VARCHAR(255) NOT NULL,
    unloading_city_gcid INTEGER NOT NULL,
    unloading_city_name VARCHAR(255) NOT NULL,
    body_group_id INTEGER NOT NULL,
    body_group_name VARCHAR(64) NOT NULL,
    weight_category VARCHAR(4) NOT NULL,
    p25_trip NUMERIC(18, 2) NOT NULL,
    median_trip NUMERIC(18, 2) NOT NULL,
    p75_trip NUMERIC(18, 2) NOT NULL,
    avg_trip NUMERIC(18, 2) NOT NULL,
    p25_km NUMERIC(18, 4) NOT NULL,
    median_km NUMERIC(18, 4) NOT NULL,
    p75_km NUMERIC(18, 4) NOT NULL,
    trips_count BIGINT NOT NULL,
    carriers_count BIGINT NOT NULL,
    cv_rate NUMERIC(18, 6),
    mom_pct NUMERIC(12, 4),
    yoy_pct NUMERIC(12, 4),
    reliability_level VARCHAR(16) NOT NULL,
    PRIMARY KEY (
        loading_month,
        loading_city_gcid,
        unloading_city_gcid,
        body_group_id,
        weight_category
    )
);

CREATE TABLE IF NOT EXISTS dm.ml_price_predictions
(
    price_id BIGINT NOT NULL,
    loading_date DATE NOT NULL,
    loading_city_gcid INTEGER NOT NULL,
    unloading_city_gcid INTEGER NOT NULL,
    body_group VARCHAR(255) NOT NULL,
    dataset_split VARCHAR(16) NOT NULL,
    actual_price_rub NUMERIC(18, 2),
    predicted_price_rub NUMERIC(18, 2) NOT NULL,
    abs_error_rub NUMERIC(18, 2),
    ape_pct NUMERIC(12, 4),
    model_name VARCHAR(128) NOT NULL,
    scored_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (price_id, model_name),
    CHECK (dataset_split IN ('test', 'future'))
);
