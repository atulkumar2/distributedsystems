CREATE TABLE IF NOT EXISTS telemetry_events (
    event_id VARCHAR(64) PRIMARY KEY,
    vehicle_id VARCHAR(64) NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    schema_version INTEGER NOT NULL DEFAULT 1,
    speed DOUBLE PRECISION NOT NULL,
    fuel_level DOUBLE PRECISION NOT NULL,
    battery_health DOUBLE PRECISION,
    odometer_km DOUBLE PRECISION,
    engine_status VARCHAR(32) NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    "partition" INTEGER NOT NULL,
    "offset" BIGINT NOT NULL
);

ALTER TABLE telemetry_events ADD COLUMN IF NOT EXISTS schema_version INTEGER NOT NULL DEFAULT 1;
ALTER TABLE telemetry_events ADD COLUMN IF NOT EXISTS battery_health DOUBLE PRECISION;
ALTER TABLE telemetry_events ADD COLUMN IF NOT EXISTS odometer_km DOUBLE PRECISION;

CREATE INDEX IF NOT EXISTS idx_telemetry_events_vehicle_time
    ON telemetry_events (vehicle_id, event_timestamp DESC, "offset" DESC);
