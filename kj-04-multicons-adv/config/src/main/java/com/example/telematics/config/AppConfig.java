package com.example.telematics.config;

/**
 * Central place for all shared Kafka / application configuration constants.
 *
 * Values can be overridden at runtime via environment variables.
 * Pattern:  System.getenv().getOrDefault("ENV_VAR", "default")
 */
public final class AppConfig {

    // ── Kafka ──────────────────────────────────────────────────────────────────

    /** Bootstrap server address. Override with env var KAFKA_BOOTSTRAP_SERVERS. */
    public static final String BOOTSTRAP_SERVERS =
            System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");

    /** Main vehicle telemetry topic — produced by the UI producer app. */
    public static final String TOPIC_TELEMETRY = "vehicle-telemetry";

    /**
     * Dead-Letter Queue topic.
     * Events that fail processing in the Storage Consumer land here instead of
     * being retried indefinitely, preventing consumer lag from growing unboundedly.
     */
    public static final String TOPIC_DLQ = "vehicle-telemetry-dlq";

    // ── Consumer groups ────────────────────────────────────────────────────────

    /**
     * Consumer group for the Storage Consumer.
     *
     * Kafka assigns topic partitions to members of the same group — each partition
     * is consumed by exactly ONE member at a time.  Using a separate group ID here
     * means the Storage Consumer has its own set of offsets, completely independent
     * from the existing UI consumer group.
     */
    public static final String GROUP_STORAGE = "telemetry-storage-group";

    /**
     * Consumer group for the Alert Consumer.
     *
     * Because this is a different group ID, Kafka will deliver every message to
     * BOTH this consumer AND the Storage Consumer — the classic fan-out pattern.
     * Neither consumer "steals" messages from the other.
     */
    public static final String GROUP_ALERT = "telemetry-alert-group";

    /** Consumer group for the DLQ Viewer service. */
    public static final String GROUP_DLQ_VIEWER = "telemetry-dlq-viewer-group";

    // ── Alert thresholds ───────────────────────────────────────────────────────

    /** Speed threshold (km/h) above which an ALERT is raised. */
    public static final double SPEED_ALERT_THRESHOLD = 100.0;

    /**
     * Speed threshold (km/h) above which the Storage Consumer simulates a
     * processing failure and routes the event to the DLQ.
     */
    public static final double SPEED_FAILURE_THRESHOLD = 120.0;

    /** Fuel level (%) below which a WARNING is raised. */
    public static final double FUEL_WARN_THRESHOLD = 20.0;

    /** Fuel level (%) below which a CRITICAL alert is raised (imminent stall risk). */
    public static final double FUEL_CRITICAL_THRESHOLD = 10.0;

    /**
     * Speed delta (km/h) between two consecutive events for the same vehicle
     * that signals a sudden-acceleration or sudden-braking event.
     * VehicleSimulator's SUDDEN_BRAKE / SUDDEN_ACCELERATION states produce deltas in this range.
     */
    public static final double SPEED_SUDDEN_CHANGE_KMH = 40.0;

    // ── Geofence (Bangalore bounding box — matches VehicleSimulator coordinate range) ────────────

    /** Geographic bounding box for fleet geofence monitoring. */
    public static final double GEO_LAT_MIN = 12.88;
    public static final double GEO_LAT_MAX = 13.12;
    public static final double GEO_LNG_MIN = 77.48;
    public static final double GEO_LNG_MAX = 77.72;

    // ── Vehicle offline detection ──────────────────────────────────────────────

    /**
     * Seconds of silence from a vehicle before it is considered offline.
     * The watcher fires every 5 s, so effective detection latency is 10–15 s.
     */
    public static final int VEHICLE_OFFLINE_SECONDS = 10;

    // ── Retry ──────────────────────────────────────────────────────────────────

    /** Number of times to retry processing before sending an event to the DLQ. */
    public static final int MAX_RETRIES = 3;

    /** Initial retry backoff in milliseconds for transient processing failures. */
    public static final int RETRY_BACKOFF_MS = 250;

    // ── Lag simulation ─────────────────────────────────────────────────────────

    /**
     * Set env var SLOW_MODE=true to enable artificial per-message delay.
     * This is useful for observing consumer lag grow in the Kafka UI.
     */
    public static final boolean SLOW_MODE =
            "true".equalsIgnoreCase(System.getenv("SLOW_MODE"));

    /** Minimum sleep time (ms) per message when SLOW_MODE is enabled. */
    public static final int SLOW_MODE_MIN_MS = 200;

    /** Maximum sleep time (ms) per message when SLOW_MODE is enabled. */
    public static final int SLOW_MODE_MAX_MS = 500;

    private AppConfig() {}
}
