package com.example.telematics.model;

import java.time.Instant;
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

    /** Convenience factory — creates a randomised event for testing. */
    public static TelemetryEvent random() {
        Random r = new Random();
        String[] vehicles = {"VH-1001", "VH-1002", "VH-1003", "VH-1004", "VH-1005"};
        String[] statuses  = {"ON", "ON", "ON", "IDLE"};
        return new TelemetryEvent(
            vehicles[r.nextInt(vehicles.length)],
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
