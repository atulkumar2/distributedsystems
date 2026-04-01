package com.example.telematics.model;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Represents a single vehicle telemetry event published to the Kafka topic.
 * Plain POJO — no framework annotations so it compiles without Spring on the classpath.
 */
public class TelemetryEvent {

    private String vehicleId;
    private String timestamp;   // ISO-8601, e.g. "2026-03-31T10:15:30Z"
    private double latitude;
    private double longitude;
    private double speed;       // km/h
    private double fuelLevel;   // percent 0–100
    private String engineStatus; // "ON" | "OFF" | "IDLE"

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
        Random r = new Random();
        String[] statuses  = {"ON", "ON", "ON", "IDLE"};
        return new TelemetryEvent(
            vehicleIds.get(r.nextInt(vehicleIds.size())),
            Instant.now().toString(),
            12.90 + r.nextDouble() * 0.20,   // Bangalore-ish latitude
            77.50 + r.nextDouble() * 0.20,
            r.nextDouble() * 140,             // intentionally sometimes > 120 to trigger DLQ
            r.nextDouble() * 100,             // fuel 0–100
            statuses[r.nextInt(statuses.length)]
        );
    }

    // ── Getters / Setters ─────────────────────────────────────────────────────

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
}
