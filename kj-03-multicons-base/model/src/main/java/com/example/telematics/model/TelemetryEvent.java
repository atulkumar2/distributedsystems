package com.example.telematics.model;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * Represents a single vehicle telemetry event published to the Kafka topic.
 * Plain POJO — no framework annotations so it compiles without Spring on the classpath.
 */
public class TelemetryEvent {

    private String eventId;
    private String vehicleId;
    private String timestamp;   // ISO-8601, e.g. "2026-03-31T10:15:30Z"
    private double latitude;
    private double longitude;
    private double speed;       // km/h
    private double fuelLevel;   // percent 0–100
    private String engineStatus; // "ON" | "OFF" | "IDLE"
    private Integer schemaVersion; // v1 base contract, v2 adds optional fields
    private Double batteryHealth;  // percent 0–100 (v2)
    private Double odometerKm;     // cumulative distance in km (v2)

    public TelemetryEvent() {}

    public TelemetryEvent(String vehicleId, String timestamp, double latitude,
                          double longitude, double speed, double fuelLevel,
                          String engineStatus) {
        this.vehicleId     = vehicleId;
        this.timestamp     = timestamp;
        this.latitude      = latitude;
        this.longitude     = longitude;
        this.speed         = speed;
        this.fuelLevel     = fuelLevel;
        this.engineStatus  = engineStatus;
        this.schemaVersion = 1;
    }

    public TelemetryEvent(String vehicleId, String timestamp, double latitude,
                          double longitude, double speed, double fuelLevel,
                          String engineStatus, Integer schemaVersion,
                          Double batteryHealth, Double odometerKm) {
        this.vehicleId      = vehicleId;
        this.timestamp      = timestamp;
        this.latitude       = latitude;
        this.longitude      = longitude;
        this.speed          = speed;
        this.fuelLevel      = fuelLevel;
        this.engineStatus   = engineStatus;
        this.schemaVersion  = schemaVersion;
        this.batteryHealth  = batteryHealth;
        this.odometerKm     = odometerKm;
        normaliseSchema();
    }

    // ── Vehicle registry ──────────────────────────────────────────────────────
    // Loaded once at class init from vehicles.txt on the classpath.
    // All random event factories draw vehicle IDs from this list.
    private static final List<String> VEHICLE_IDS = loadVehicleIds();

    private static List<String> loadVehicleIds() {
        try (InputStream is = TelemetryEvent.class.getResourceAsStream("/vehicles.txt");
             BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
            List<String> ids = new ArrayList<>();
            br.lines().map(String::trim).filter(l -> !l.isEmpty()).forEach(ids::add);
            return Collections.unmodifiableList(ids);
        } catch (Exception e) {
            // Fallback if the resource file is missing from the classpath
            return List.of("VH-0001", "VH-0002", "VH-0003", "VH-0004", "VH-0005");
        }
    }

    /** Returns the full list of known vehicle IDs loaded from vehicles.txt. */
    public static List<String> getVehicleIds() { return VEHICLE_IDS; }

    /** Convenience factory — creates a randomised event using a random vehicle from the full list. */
    public static TelemetryEvent random() {
        return randomFrom(VEHICLE_IDS);
    }

    /**
     * Like {@link #random()} but draws the vehicle ID from the supplied subset.
     * Useful for burst scenarios where only the first N vehicles should be active.
     */
    public static TelemetryEvent randomFrom(List<String> vehicleIds) {
        return randomFrom(vehicleIds, 1);
    }

    /**
     * Creates random events for a specific schema contract version.
     * v1 emits the base fields; v2 also emits batteryHealth and odometerKm.
     */
    public static TelemetryEvent randomFrom(List<String> vehicleIds, int schemaVersion) {
        Random r = new Random();
        String[] statuses  = {"ON", "ON", "ON", "IDLE"};
        TelemetryEvent event = new TelemetryEvent(
            vehicleIds.get(r.nextInt(vehicleIds.size())),
            Instant.now().toString(),
            round6(12.90 + r.nextDouble() * 0.20),   // Bangalore-ish latitude
            round6(77.50 + r.nextDouble() * 0.20),
            round1(r.nextDouble() * 140),             // intentionally sometimes > 120 to trigger DLQ
            round1(r.nextDouble() * 100),             // fuel 0–100
            statuses[r.nextInt(statuses.length)]
        );
        if (schemaVersion >= 2) {
            event.setSchemaVersion(2);
            event.setBatteryHealth(round1(70 + r.nextDouble() * 30));      // 70–100%
            event.setOdometerKm(round1(5_000 + r.nextDouble() * 195_000)); // 5k–200k km
        } else {
            event.setSchemaVersion(1);
        }
        event.ensureEventId();
        event.normaliseSchema();
        return event;
    }

    // ── Getters / Setters ─────────────────────────────────────────────────────

    public String getEventId()                { return eventId; }
    public void   setEventId(String e)        { this.eventId = e; }

    public String getVehicleId()               { return vehicleId; }
    public void   setVehicleId(String v)       { this.vehicleId = v; }

    public String getTimestamp()               { return timestamp; }
    public void   setTimestamp(String t)       { this.timestamp = t; }

    public double getLatitude()                { return latitude; }
    public void   setLatitude(double l)        { this.latitude = l; }

    public double getLongitude()               { return longitude; }
    public void   setLongitude(double l)       { this.longitude = l; }

    public double getSpeed()                   { return speed; }
    public void   setSpeed(double s)           { this.speed = s; }

    public double getFuelLevel()               { return fuelLevel; }
    public void   setFuelLevel(double f)       { this.fuelLevel = f; }

    public String getEngineStatus()            { return engineStatus; }
    public void   setEngineStatus(String s)    { this.engineStatus = s; }

    public Integer getSchemaVersion()          { return schemaVersion == null ? 1 : schemaVersion; }
    public void    setSchemaVersion(Integer v) { this.schemaVersion = v; }

    public Double getBatteryHealth()            { return batteryHealth; }
    public void   setBatteryHealth(Double v)    { this.batteryHealth = v; }

    public Double getOdometerKm()               { return odometerKm; }
    public void   setOdometerKm(Double v)       { this.odometerKm = v; }

    public void ensureEventId() {
        if (eventId == null || eventId.isBlank()) {
            eventId = UUID.randomUUID().toString();
        }
    }

    /**
     * Keeps payloads backward compatible when some producers omit schemaVersion.
     * If extra v2 fields are present with no version set, infer v2.
     */
    public void normaliseSchema() {
        if (schemaVersion == null) {
            schemaVersion = (batteryHealth != null || odometerKm != null) ? 2 : 1;
        }
        if (schemaVersion <= 1) {
            schemaVersion = 1;
            batteryHealth = null;
            odometerKm = null;
        } else {
            schemaVersion = 2;
        }
    }

    private static double round1(double v) { return Math.round(v * 10d) / 10d; }
    private static double round6(double v) { return Math.round(v * 1_000_000d) / 1_000_000d; }
}
