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
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
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

    private final AtomicLong alertCount    = new AtomicLong(0);
    private final AtomicLong warningCount  = new AtomicLong(0);
    private final AtomicLong processedCount = new AtomicLong(0);
    private final Random random = new Random();

    // ── ApplicationRunner — kick off the poll thread on startup ───────────────

    @Override
    public void run(ApplicationArguments args) {
        Thread pollThread = new Thread(this::pollLoop, "alert-kafka-poll");
        pollThread.setDaemon(true);
        pollThread.start();
        log.info("Alert Kafka poll thread started — group={} topic={}",
                AppConfig.GROUP_ALERT, AppConfig.TOPIC_TELEMETRY);
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
            String eventId = UUID.randomUUID().toString();

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

        if (event.getSpeed() > AppConfig.SPEED_ALERT_THRESHOLD) {
            log.warn("ALERT: Overspeed vehicle={} speed={} [eventId={}][partition={}][offset={}]",
                    event.getVehicleId(), event.getSpeed(), eventId, partition, offset);
            alertCount.incrementAndGet();
            pushToSse("ALERT", event);
            anyAlert = true;
        }

        if (event.getFuelLevel() < AppConfig.FUEL_WARN_THRESHOLD) {
            log.warn("WARNING: Low fuel vehicle={} fuel={}% [eventId={}][partition={}][offset={}]",
                    event.getVehicleId(), event.getFuelLevel(), eventId, partition, offset);
            warningCount.incrementAndGet();
            pushToSse("WARNING", event);
            anyAlert = true;
        }

        if (!anyAlert) {
            log.debug("[eventId={}][vehicleId={}] No alerts — vehicle nominal.", eventId, event.getVehicleId());
            pushToSse("OK", event);
        }
    }

    // ── SSE push ──────────────────────────────────────────────────────────────

    /**
     * Pushes {"type":"ALERT|WARNING|OK","vehicleId":"...","speed":N,"fuelLevel":N}
     * to all connected browser clients.
     */
    private void pushToSse(String type, TelemetryEvent event) {
        if (emitters.isEmpty()) return;
        String payload;
        try {
            Map<String, Object> envelope = new HashMap<>();
            envelope.put("type",      type);
            envelope.put("vehicleId", event.getVehicleId());
            envelope.put("speed",     event.getSpeed());
            envelope.put("fuelLevel", event.getFuelLevel());
            envelope.put("timestamp", event.getTimestamp());
            payload = JsonUtil.toJson(envelope);
        } catch (Exception e) {
            log.warn("Failed to serialise SSE alert payload: {}", e.getMessage());
            return;
        }
        List<SseEmitter> dead = new ArrayList<>();
        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(SseEmitter.event().data(payload));
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
            log.info("[METRICS] processed={} alerts={} warnings={}",
                    processedCount.get(), alertCount.get(), warningCount.get());
        }
    }

    // ── Public API for AlertController ────────────────────────────────────────

    public Map<String, Object> getMetrics() {
        Map<String, Object> m = new HashMap<>();
        m.put("processed", processedCount.get());
        m.put("alerts",    alertCount.get());
        m.put("warnings",  warningCount.get());
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
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,        "earliest");
        // Alert consumer is a read-only observer — auto-commit is acceptable.
        // Losing an occasional alert on a crash does not cause data corruption.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,       "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,  "1000");
        return props;
    }
}
