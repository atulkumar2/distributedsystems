package com.example.telematics.model;

import java.time.Instant;
import java.util.Random;

public class TelemetryEvent {

    private String vehicleId;
    private String timestamp;
    private double latitude;
    private double longitude;
    private double speed;
    private double fuelLevel;
    private String engineStatus;

    public TelemetryEvent() {}

    public TelemetryEvent(String vehicleId, String timestamp, double latitude,
                          double longitude, double speed, double fuelLevel, String engineStatus) {
        this.vehicleId = vehicleId;
        this.timestamp = timestamp;
        this.latitude = latitude;
        this.longitude = longitude;
        this.speed = speed;
        this.fuelLevel = fuelLevel;
        this.engineStatus = engineStatus;
    }

    /** Creates a randomised telemetry event for quick testing. */
    public static TelemetryEvent random() {
        Random r = new Random();
        String[] vehicles = {"VH-1001", "VH-1002", "VH-1003", "VH-1004", "VH-1005"};
        String[] statuses = {"ON", "ON", "ON", "IDLE"};  // weight towards ON
        return new TelemetryEvent(
            vehicles[r.nextInt(vehicles.length)],
            Instant.now().toString(),
            12.90 + r.nextDouble() * 0.20,   // Bangalore-ish latitude band
            77.50 + r.nextDouble() * 0.20,
            r.nextDouble() * 120,
            20 + r.nextDouble() * 80,
            statuses[r.nextInt(statuses.length)]
        );
    }

    // ── Getters / Setters ─────────────────────────────────────────────────────

    public String getVehicleId() { return vehicleId; }
    public void setVehicleId(String vehicleId) { this.vehicleId = vehicleId; }

    public String getTimestamp() { return timestamp; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }

    public double getLatitude() { return latitude; }
    public void setLatitude(double latitude) { this.latitude = latitude; }

    public double getLongitude() { return longitude; }
    public void setLongitude(double longitude) { this.longitude = longitude; }

    public double getSpeed() { return speed; }
    public void setSpeed(double speed) { this.speed = speed; }

    public double getFuelLevel() { return fuelLevel; }
    public void setFuelLevel(double fuelLevel) { this.fuelLevel = fuelLevel; }

    public String getEngineStatus() { return engineStatus; }
    public void setEngineStatus(String engineStatus) { this.engineStatus = engineStatus; }
}
