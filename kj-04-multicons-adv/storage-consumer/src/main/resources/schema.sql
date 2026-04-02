CREATE TABLE IF NOT EXISTS telemetry_events (
    event_id VARCHAR(64) PRIMARY KEY,
    vehicle_id VARCHAR(64) NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    speed DOUBLE PRECISION NOT NULL,
    fuel_level DOUBLE PRECISION NOT NULL,
    engine_status VARCHAR(32) NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    "partition" INTEGER NOT NULL,
    "offset" BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_telemetry_events_vehicle_time
    ON telemetry_events (vehicle_id, event_timestamp DESC, "offset" DESC);
