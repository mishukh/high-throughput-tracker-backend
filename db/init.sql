CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS assets (
    id VARCHAR(50) PRIMARY KEY,
    driver_name VARCHAR(100),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS location_history (
    time TIMESTAMPTZ NOT NULL,
    asset_id VARCHAR(50) REFERENCES assets(id),
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    UNIQUE (asset_id, time)
);

SELECT create_hypertable('location_history', by_range('time'));

CREATE INDEX ix_location_history_asset_time ON location_history (asset_id, time DESC);

INSERT INTO assets (id, driver_name) VALUES 
('asset-1', 'Alice'),
('asset-2', 'Bob'),
('asset-3', 'Charlie')
ON CONFLICT DO NOTHING;

ALTER TABLE location_history SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'asset_id'
);
SELECT add_compression_policy('location_history', INTERVAL '1 hour');

CREATE MATERIALIZED VIEW hourly_asset_stats
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 hour', time) AS bucket,
       asset_id,
       count(*) as ping_count,
       last(latitude, time) as last_lat,
       last(longitude, time) as last_lng
FROM location_history
GROUP BY bucket, asset_id;

SELECT add_continuous_aggregate_policy('hourly_asset_stats',
    start_offset => INTERVAL '3 hours',
    end_offset => INTERVAL '15 minutes',
    schedule_interval => INTERVAL '15 minutes');

SELECT add_retention_policy('location_history', INTERVAL '6 months');
