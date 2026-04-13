package com.example.telematics.storage;

import com.example.telematics.model.TelemetryEvent;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Repository
public class TelemetryEventRepository {

    private static final String INSERT_SQL = """
            INSERT INTO telemetry_events (
                event_id,
                vehicle_id,
                event_timestamp,
                schema_version,
                speed,
                fuel_level,
                battery_health,
                odometer_km,
                engine_status,
                latitude,
                longitude,
                "partition",
                "offset"
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (event_id) DO NOTHING
            """;

    private static final String LATEST_SQL = """
            SELECT event_id,
                   vehicle_id,
                   event_timestamp,
                     schema_version,
                   speed,
                   fuel_level,
                     battery_health,
                     odometer_km,
                   engine_status,
                   latitude,
                   longitude
            FROM (
                SELECT event_id,
                       vehicle_id,
                       event_timestamp,
                      schema_version,
                       speed,
                       fuel_level,
                      battery_health,
                      odometer_km,
                       engine_status,
                       latitude,
                       longitude,
                       ROW_NUMBER() OVER (
                           PARTITION BY vehicle_id
                           ORDER BY event_timestamp DESC, "offset" DESC
                       ) AS rn
                FROM telemetry_events
            ) ranked
            WHERE rn = 1
            ORDER BY vehicle_id
            """;

    private final JdbcTemplate jdbcTemplate;

    public TelemetryEventRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public boolean insert(TelemetryEvent event, int partition, long offset) {
        int rows = jdbcTemplate.update(
                INSERT_SQL,
                event.getEventId(),
                event.getVehicleId(),
                Timestamp.from(Instant.parse(event.getTimestamp())),
                event.getSchemaVersion(),
                event.getSpeed(),
                event.getFuelLevel(),
                event.getBatteryHealth(),
                event.getOdometerKm(),
                event.getEngineStatus(),
                event.getLatitude(),
                event.getLongitude(),
                partition,
                offset
        );
        return rows == 1;
    }

    public Map<String, TelemetryEvent> loadLatestByVehicle() {
        List<TelemetryEvent> rows = jdbcTemplate.query(LATEST_SQL, (rs, rowNum) -> mapEvent(rs));
        Map<String, TelemetryEvent> latest = new LinkedHashMap<>();
        for (TelemetryEvent event : rows) {
            latest.put(event.getVehicleId(), event);
        }
        return latest;
    }

    private TelemetryEvent mapEvent(ResultSet rs) throws SQLException {
        TelemetryEvent event = new TelemetryEvent();
        event.setEventId(rs.getString("event_id"));
        event.setVehicleId(rs.getString("vehicle_id"));
        event.setTimestamp(rs.getTimestamp("event_timestamp").toInstant().toString());
        event.setSchemaVersion(readNullableInt(rs, "schema_version"));
        event.setSpeed(rs.getDouble("speed"));
        event.setFuelLevel(rs.getDouble("fuel_level"));
        event.setBatteryHealth(readNullableDouble(rs, "battery_health"));
        event.setOdometerKm(readNullableDouble(rs, "odometer_km"));
        event.setEngineStatus(rs.getString("engine_status"));
        event.setLatitude(rs.getDouble("latitude"));
        event.setLongitude(rs.getDouble("longitude"));
        event.normaliseSchema();
        return event;
    }

    private Integer readNullableInt(ResultSet rs, String column) throws SQLException {
        int value = rs.getInt(column);
        return rs.wasNull() ? null : value;
    }

    private Double readNullableDouble(ResultSet rs, String column) throws SQLException {
        double value = rs.getDouble(column);
        return rs.wasNull() ? null : value;
    }
}
