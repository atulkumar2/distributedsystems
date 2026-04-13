package com.example.telematics.alert;

import com.example.telematics.config.AppConfig;
import com.example.telematics.model.TelemetryEvent;
import com.example.telematics.util.JsonUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * AlertService — Kafka consumer running on a background daemon thread that
 * evaluates telemetry events against alert thresholds and pushes live
 * notifications to browser clients via Server-Sent Events.
 *
 * Fan-out teaching point:
 *  Uses AppConfig.GROUP_ALERT — a completely separate consumer group from the
 *  Storage Consumer.  Kafka delivers every message to BOTH groups independently:
 *  this is the publish-subscribe (fan-out) pattern.  Neither consumer "steals"
 *  messages from the other.
 *
 * Auto-commit vs manual commit:
 *  Alert consumer uses enable.auto.commit=true because it is a read-only observer.
 *  Losing an occasional alert on a crash is acceptable.  Contrast with the Storage
 *  Consumer (manual commit) where data loss is not acceptable.
 */
@Component
public class AlertService implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(AlertService.class);

    // Active SSE connections from browsers
    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();

    private final AtomicLong alertCount         = new AtomicLong(0);
    private final AtomicLong warningCount       = new AtomicLong(0);
    private final AtomicLong criticalCount      = new AtomicLong(0);
    private final AtomicLong engineAnomalyCount = new AtomicLong(0);
    private final AtomicLong suddenChangeCount  = new AtomicLong(0);
    private final AtomicLong geofenceCount      = new AtomicLong(0);
    private final AtomicLong processedCount     = new AtomicLong(0);
    private final Random random = new Random();

    // Per-vehicle last-known speed for sudden-change detection across consecutive events.
    // Written and read only by the single alert-kafka-poll thread, so a plain HashMap is fine.
    private final Map<String, Double> lastSpeedByVehicle = new HashMap<>();

    // ConcurrentHashMap — shared between the poll thread (writes) and the offline-watcher thread (reads).
    // lastSeenMs: epoch-ms of the last event received per vehicle.
    // offlineAlerted: vehicles for which we have already fired a vehicle-offline alert; cleared when
    //                 the vehicle resumes sending so the alert can fire again if it goes silent again.
    private final Map<String, Long> lastSeenMs     = new ConcurrentHashMap<>();
    private final Set<String>       offlineAlerted = ConcurrentHashMap.newKeySet();
    private final AtomicLong        offlineCount   = new AtomicLong(0);

    // ── ApplicationRunner — kick off the poll thread on startup ───────────────

    @Override
    public void run(ApplicationArguments args) {
        Thread pollThread = new Thread(this::pollLoop, "alert-kafka-poll");
        pollThread.setDaemon(true);
        pollThread.start();
        log.info("Alert Kafka poll thread started — group={} topic={}",
                AppConfig.GROUP_ALERT, AppConfig.TOPIC_TELEMETRY);

        startOfflineWatcher();
    }

    // ── Kafka poll loop ───────────────────────────────────────────────────────

    private void pollLoop() {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(buildConsumerProps())) {
            consumer.subscribe(Collections.singletonList(AppConfig.TOPIC_TELEMETRY));

            while (!Thread.currentThread().isInterrupted()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    handleRecord(record);
                }
            }
        } catch (org.apache.kafka.common.errors.WakeupException e) {
            log.info("Alert poll loop received wakeup — stopping.");
        } catch (Exception e) {
            log.error("Alert poll loop terminated unexpectedly: {}", e.getMessage(), e);
        }
    }

    // ── Record handler ────────────────────────────────────────────────────────

    private void handleRecord(ConsumerRecord<String, String> record) {
        applySlowMode();
        try {
            TelemetryEvent event = JsonUtil.fromJson(record.value(), TelemetryEvent.class);
            event.normaliseSchema();
            if (event.getVehicleId() == null || event.getVehicleId().isBlank()) {
                log.warn("[partition={}][offset={}] Skipping alert evaluation: missing vehicleId", record.partition(), record.offset());
                return;
            }
            String eventId = UUID.randomUUID().toString();

            // Mark vehicle as alive; remove any pending offline alert so it can re-arm later.
            lastSeenMs.put(event.getVehicleId(), System.currentTimeMillis());
            offlineAlerted.remove(event.getVehicleId());

            log.info("[eventId={}][vehicleId={}][partition={}][offset={}] Evaluating speed={} fuel={}",
                    eventId, event.getVehicleId(), record.partition(), record.offset(),
                    event.getSpeed(), event.getFuelLevel());

            evaluate(event, eventId, record.partition(), record.offset());
            processedCount.incrementAndGet();
            logMetrics();

        } catch (Exception e) {
            log.error("[partition={}][offset={}] Failed to process alert record: {}",
                    record.partition(), record.offset(), e.getMessage());
        }
        // auto-commit handles offset advancement — no explicit commitSync() needed
    }

    // ── Alert evaluation ──────────────────────────────────────────────────────

    private void evaluate(TelemetryEvent event, String eventId,
                          int partition, long offset) {
        boolean anyAlert = false;

        // Retrieve-and-update last speed for this vehicle (null on first event).
        // put() returns the old value atomically, giving us the previous reading.
        Double lastSpeed = lastSpeedByVehicle.put(event.getVehicleId(), event.getSpeed());

        // ── Rule 1 & 2: Overspeed (tiered) ────────────────────────────────────
        if (event.getSpeed() > AppConfig.SPEED_FAILURE_THRESHOLD) {
            // CRITICAL: above the DLQ failure threshold (120 km/h)
            log.warn("CRITICAL: Critical overspeed vehicle={} speed={} [eventId={}][partition={}][offset={}]",
                    event.getVehicleId(), event.getSpeed(), eventId, partition, offset);
            criticalCount.incrementAndGet();
            pushToSse("CRITICAL", "critical-speed", event, 0);
            anyAlert = true;
        } else if (event.getSpeed() > AppConfig.SPEED_ALERT_THRESHOLD) {
            // ALERT: 100–120 km/h range
            log.warn("ALERT: Overspeed vehicle={} speed={} [eventId={}][partition={}][offset={}]",
                    event.getVehicleId(), event.getSpeed(), eventId, partition, offset);
            alertCount.incrementAndGet();
            pushToSse("ALERT", "overspeed", event, 0);
            anyAlert = true;
        }

        // ── Rule 3 & 4: Fuel level (tiered) ───────────────────────────────────
        if (event.getFuelLevel() < AppConfig.FUEL_CRITICAL_THRESHOLD) {
            // CRITICAL: < 10 % — imminent stall risk
            log.warn("CRITICAL: Critical fuel vehicle={} fuel={}% [eventId={}][partition={}][offset={}]",
                    event.getVehicleId(), event.getFuelLevel(), eventId, partition, offset);
            criticalCount.incrementAndGet();
            pushToSse("CRITICAL", "critical-fuel", event, 0);
            anyAlert = true;
        } else if (event.getFuelLevel() < AppConfig.FUEL_WARN_THRESHOLD) {
            // WARNING: 10–20 %
            log.warn("WARNING: Low fuel vehicle={} fuel={}% [eventId={}][partition={}][offset={}]",
                    event.getVehicleId(), event.getFuelLevel(), eventId, partition, offset);
            warningCount.incrementAndGet();
            pushToSse("WARNING", "low-fuel", event, 0);
            anyAlert = true;
        }

        // ── Rule 5: Engine anomaly — vehicle moving with engine OFF or IDLE ───
        if (event.getSpeed() > 5.0
                && ("OFF".equals(event.getEngineStatus()) || "IDLE".equals(event.getEngineStatus()))) {
            log.warn("WARNING: Engine anomaly vehicle={} speed={} engine={} [eventId={}]",
                    event.getVehicleId(), event.getSpeed(), event.getEngineStatus(), eventId);
            engineAnomalyCount.incrementAndGet();
            pushToSse("WARNING", "engine-anomaly", event, 0);
            anyAlert = true;
        }

        // ── Rule 6: Sudden speed change (delta across consecutive events) ─────
        if (lastSpeed != null) {
            double delta = event.getSpeed() - lastSpeed;
            if (Math.abs(delta) > AppConfig.SPEED_SUDDEN_CHANGE_KMH) {
                String dir = delta > 0 ? "acceleration" : "deceleration";
                log.warn("WARNING: Sudden {} vehicle={} delta={}km/h [eventId={}]",
                        dir, event.getVehicleId(), String.format("%.1f", delta), eventId);
                suddenChangeCount.incrementAndGet();
                pushToSse("WARNING", "sudden-change", event, delta);
                anyAlert = true;
            }
        }

        // ── Rule 7: Geofence — vehicle outside Bangalore bounding box ─────────
        if (event.getLatitude()  < AppConfig.GEO_LAT_MIN || event.getLatitude()  > AppConfig.GEO_LAT_MAX
                || event.getLongitude() < AppConfig.GEO_LNG_MIN || event.getLongitude() > AppConfig.GEO_LNG_MAX) {
            log.warn("WARNING: Geofence breach vehicle={} lat={} lng={} [eventId={}]",
                    event.getVehicleId(), event.getLatitude(), event.getLongitude(), eventId);
            geofenceCount.incrementAndGet();
            pushToSse("WARNING", "geofence", event, 0);
            anyAlert = true;
        }

        if (!anyAlert) {
            log.debug("[eventId={}][vehicleId={}] Nominal — no alerts.", eventId, event.getVehicleId());
            pushToSse("OK", "nominal", event, 0);
        }
    }

    // ── Offline vehicle watcher ───────────────────────────────────────────────

    /**
     * Schedules a background task that fires every 5 s and checks whether any
     * previously-seen vehicle has gone silent.  Uses a single-thread daemon
     * ScheduledExecutorService so it never blocks the Kafka poll thread.
     */
    private void startOfflineWatcher() {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "alert-offline-watcher");
            t.setDaemon(true);
            return t;
        });
        // Initial delay of 15 s so we don't fire on startup before any events arrive.
        scheduler.scheduleAtFixedRate(this::checkOfflineVehicles,
                15, 5, TimeUnit.SECONDS);
        log.info("Offline vehicle watcher scheduled — silence threshold={}s",
                AppConfig.VEHICLE_OFFLINE_SECONDS);
    }

    /**
     * Iterates all known vehicles and fires a one-shot vehicle-offline SSE
     * for each one that has been silent longer than VEHICLE_OFFLINE_SECONDS.
     * offlineAlerted.add() acts as a gate: it returns true only on the first
     * call, preventing repeated alerts until the vehicle resumes (at which point
     * handleRecord clears it from the set).
     */
    private void checkOfflineVehicles() {
        long now = System.currentTimeMillis();
        long thresholdMs = (long) AppConfig.VEHICLE_OFFLINE_SECONDS * 1000;
        for (Map.Entry<String, Long> entry : lastSeenMs.entrySet()) {
            String vehicleId = entry.getKey();
            long silentMs = now - entry.getValue();
            if (silentMs > thresholdMs && offlineAlerted.add(vehicleId)) {
                log.warn("WARNING: Vehicle went offline vehicle={} silentFor={}s",
                        vehicleId, silentMs / 1000);
                offlineCount.incrementAndGet();
                pushOfflineToSse(vehicleId, silentMs / 1000);
            }
        }
    }

    /** Pushes a vehicle-offline envelope to all SSE clients (no telemetry fields — vehicle is silent). */
    private void pushOfflineToSse(String vehicleId, long silentSeconds) {
        if (emitters.isEmpty()) return;
        String payload;
        try {
            Map<String, Object> envelope = new HashMap<>();
            envelope.put("type",          "WARNING");
            envelope.put("rule",          "vehicle-offline");
            envelope.put("vehicleId",     vehicleId);
            envelope.put("silentSeconds", silentSeconds);
            payload = Objects.requireNonNull(JsonUtil.toJson(envelope));
        } catch (Exception e) {
            log.warn("Failed to serialise offline SSE payload: {}", e.getMessage());
            return;
        }
        List<SseEmitter> dead = new ArrayList<>();
        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(payload);
            } catch (IOException e) {
                dead.add(emitter);
            }
        }
        emitters.removeAll(dead);
    }

    // ── SSE push ──────────────────────────────────────────────────────────────

    /**
     * Pushes a JSON alert envelope to all connected browser SSE clients.
     * Fields: type, rule, vehicleId, speed, fuelLevel, engineStatus, timestamp,
     *         and delta (only when non-zero, i.e. for sudden-change alerts).
     */
    private void pushToSse(String type, String rule, TelemetryEvent event, double delta) {
        if (emitters.isEmpty()) return;
        String payload;
        try {
            Map<String, Object> envelope = new HashMap<>();
            envelope.put("type",         type);
            envelope.put("rule",         rule);
            envelope.put("vehicleId",    event.getVehicleId());
            envelope.put("schemaVersion", event.getSchemaVersion());
            envelope.put("speed",        event.getSpeed());
            envelope.put("fuelLevel",    event.getFuelLevel());
            if (event.getBatteryHealth() != null) envelope.put("batteryHealth", event.getBatteryHealth());
            if (event.getOdometerKm() != null) envelope.put("odometerKm", event.getOdometerKm());
            envelope.put("engineStatus", event.getEngineStatus());
            envelope.put("timestamp",    event.getTimestamp());
            if (delta != 0) envelope.put("delta", delta);
            payload = Objects.requireNonNull(JsonUtil.toJson(envelope));
        } catch (Exception e) {
            log.warn("Failed to serialise SSE alert payload: {}", e.getMessage());
            return;
        }
        List<SseEmitter> dead = new ArrayList<>();
        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(payload);
            } catch (IOException e) {
                dead.add(emitter);
            }
        }
        emitters.removeAll(dead);
    }

    // ── Slow mode ─────────────────────────────────────────────────────────────

    private void applySlowMode() {
        if (!AppConfig.SLOW_MODE) return;
        int sleepMs = AppConfig.SLOW_MODE_MIN_MS
                + random.nextInt(AppConfig.SLOW_MODE_MAX_MS - AppConfig.SLOW_MODE_MIN_MS);
        try {
            Thread.sleep(sleepMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ── Metrics ───────────────────────────────────────────────────────────────

    private void logMetrics() {
        if (processedCount.get() % 10 == 0) {
            log.info("[METRICS] processed={} criticals={} alerts={} warnings={} engineAnomalies={} suddenChanges={} geofence={} offline={}",
                    processedCount.get(), criticalCount.get(), alertCount.get(), warningCount.get(),
                    engineAnomalyCount.get(), suddenChangeCount.get(), geofenceCount.get(), offlineCount.get());
        }
    }

    // ── Public API for AlertController ────────────────────────────────────────

    public Map<String, Object> getMetrics() {
        Map<String, Object> m = new HashMap<>();
        m.put("processed",       processedCount.get());
        m.put("criticals",       criticalCount.get());
        m.put("alerts",          alertCount.get());
        m.put("warnings",        warningCount.get());
        m.put("engineAnomalies", engineAnomalyCount.get());
        m.put("suddenChanges",   suddenChangeCount.get());
        m.put("geofence",        geofenceCount.get());
        m.put("offline",         offlineCount.get());
        return m;
    }

    /** Called by AlertController to register a new browser SSE connection. */
    public SseEmitter subscribe() {
        SseEmitter emitter = new SseEmitter(0L);
        emitters.add(emitter);
        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(()    -> emitters.remove(emitter));
        emitter.onError(e       -> emitters.remove(emitter));
        log.info("New SSE subscriber (alert) — active: {}", emitters.size());
        return emitter;
    }

    // ── Consumer properties ───────────────────────────────────────────────────

    private Properties buildConsumerProps() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,        AppConfig.BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,                 AppConfig.GROUP_ALERT);
        // Fixed client-id: visible in Kafka broker logs and metrics under a stable name
        // instead of the broker-assigned random id, which changes on every restart.
        props.put(ConsumerConfig.CLIENT_ID_CONFIG,                "alert-consumer-1");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,        "latest");
        // Alert consumer is a read-only observer — auto-commit is acceptable.
        // Losing an occasional alert on a crash does not cause data corruption.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,       "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,  "1000");
        return props;
    }
}
