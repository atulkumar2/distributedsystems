package com.example.telematics.storage;

import com.example.telematics.config.AppConfig;
import com.example.telematics.dlq.DlqProducer;
import com.example.telematics.model.TelemetryEvent;
import com.example.telematics.util.JsonUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * StorageService — runs the Kafka poll loop on a background daemon thread so it
 * does not block Spring Boot startup, while exposing an SSE subscription API for
 * the browser to receive live event updates.
 *
 * Key teaching points preserved from the original plain-Java consumer:
 *  - Manual offset commit (enable.auto.commit = false) — offset advances only
 *    after successful storage, giving at-least-once delivery semantics.
 *  - Retry before DLQ — transient failures are retried up to MAX_RETRIES times.
 *  - SimulatedFailureException — speed > SPEED_FAILURE_THRESHOLD bypasses retries
 *    and goes straight to the DLQ, demonstrating the DLQ routing pattern.
 *  - Slow mode — artificial delay lets you observe consumer-group lag in Kafka UI.
 */
@Component
public class StorageService implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(StorageService.class);

    // Latest event per vehicleId (acts as our in-memory "database")
    private final Map<String, TelemetryEvent> eventStore = new ConcurrentHashMap<>();

    // Active SSE connections from browsers; thread-safe because the Kafka thread
    // writes while HTTP threads add/remove entries.
    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();

    private final AtomicLong processedCount = new AtomicLong(0);
    private final AtomicLong failureCount   = new AtomicLong(0);
    private final Random random = new Random();

    // ── ApplicationRunner — kick off the poll thread on startup ───────────────

    @Override
    public void run(ApplicationArguments args) {
        // Daemon thread: JVM exits cleanly even if the thread is still blocking on poll()
        Thread pollThread = new Thread(this::pollLoop, "storage-kafka-poll");
        pollThread.setDaemon(true);
        pollThread.start();
        log.info("Storage Kafka poll thread started — group={} topic={}",
                AppConfig.GROUP_STORAGE, AppConfig.TOPIC_TELEMETRY);
    }

    // ── Kafka poll loop ───────────────────────────────────────────────────────

    private void pollLoop() {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(buildConsumerProps());
             DlqProducer dlqProducer = new DlqProducer()) {

            consumer.subscribe(Collections.singletonList(AppConfig.TOPIC_TELEMETRY));

            while (!Thread.currentThread().isInterrupted()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    handleRecord(record, consumer, dlqProducer);
                }
            }
        } catch (org.apache.kafka.common.errors.WakeupException e) {
            log.info("Storage poll loop received wakeup — stopping.");
        } catch (Exception e) {
            log.error("Storage poll loop terminated unexpectedly: {}", e.getMessage(), e);
        }
    }

    // ── Record handler ────────────────────────────────────────────────────────

    private void handleRecord(ConsumerRecord<String, String> record,
                               KafkaConsumer<String, String> consumer,
                               DlqProducer dlqProducer) {
        // Apply slow-mode delay BEFORE processing so lag accumulates in Kafka UI
        applySlowMode();

        TelemetryEvent event = null;
        String eventId = null;
        try {
            event  = JsonUtil.fromJson(record.value(), TelemetryEvent.class);
            eventId = UUID.randomUUID().toString();

            log.info("[eventId={}][vehicleId={}][partition={}][offset={}] Received speed={} fuel={}",
                    eventId, event.getVehicleId(), record.partition(), record.offset(),
                    event.getSpeed(), event.getFuelLevel());

            processWithRetry(event, eventId, record, consumer, dlqProducer);

        } catch (Exception e) {
            failureCount.incrementAndGet();
            log.error("[partition={}][offset={}] Malformed JSON — routing to DLQ: {}",
                    record.partition(), record.offset(), e.getMessage());

            if (event == null) {
                event = new TelemetryEvent();
                event.setVehicleId(record.key() != null ? record.key() : "unknown");
                event.setTimestamp("unknown");
            }
            dlqProducer.send(event, "Malformed JSON: " + e.getMessage());
            pushToSse(event, "DLQ");
            // Commit even on parse failure so the corrupt record isn't re-delivered forever
            commitOffset(consumer, record);
        }
    }

    // ── Retry + failure simulation ────────────────────────────────────────────

    private void processWithRetry(TelemetryEvent event, String eventId,
                                   ConsumerRecord<String, String> record,
                                   KafkaConsumer<String, String> consumer,
                                   DlqProducer dlqProducer) {
        int attempt = 0;
        Exception lastException = null;

        while (attempt < AppConfig.MAX_RETRIES) {
            attempt++;
            try {
                process(event, eventId, record);
                // SUCCESS — commit offset, update metrics, push to UI
                commitOffset(consumer, record);
                processedCount.incrementAndGet();
                pushToSse(event, "STORED");
                logMetrics();
                return;
            } catch (SimulatedFailureException sfe) {
                // Speed > SPEED_FAILURE_THRESHOLD: hard failure — skip retries, go to DLQ
                log.warn("[eventId={}][vehicleId={}] Simulated failure — routing to DLQ",
                        eventId, event.getVehicleId());
                failureCount.incrementAndGet();
                dlqProducer.send(event, sfe.getMessage());
                pushToSse(event, "DLQ");
                commitOffset(consumer, record);
                return;
            } catch (Exception e) {
                lastException = e;
                log.warn("[eventId={}] Attempt {}/{} failed: {}",
                        eventId, attempt, AppConfig.MAX_RETRIES, e.getMessage());
            }
        }

        // All retries exhausted → DLQ
        failureCount.incrementAndGet();
        log.error("[eventId={}][vehicleId={}] All {} retries exhausted — routing to DLQ",
                eventId, event.getVehicleId(), AppConfig.MAX_RETRIES);
        dlqProducer.send(event, "Failed after " + AppConfig.MAX_RETRIES + " retries: "
                + (lastException != null ? lastException.getMessage() : "unknown"));
        pushToSse(event, "DLQ");
        commitOffset(consumer, record);
    }

    private void process(TelemetryEvent event, String eventId,
                         ConsumerRecord<String, String> record) {
        // Simulated failure: speed > threshold → throws, bypasses retries → DLQ
        if (event.getSpeed() > AppConfig.SPEED_FAILURE_THRESHOLD) {
            throw new SimulatedFailureException(
                    "Simulated failure: speed=" + event.getSpeed()
                    + " exceeds threshold=" + AppConfig.SPEED_FAILURE_THRESHOLD);
        }
        // Latest-write-wins: only the most recent event per vehicle is retained
        eventStore.put(event.getVehicleId(), event);

        log.info("[eventId={}][vehicleId={}][partition={}][offset={}] Stored — store_size={}",
                eventId, event.getVehicleId(), record.partition(), record.offset(), eventStore.size());
    }

    // ── Manual offset commit ──────────────────────────────────────────────────

    /**
     * Commit offset = record.offset() + 1: tells Kafka the next record we want.
     * commitSync() blocks until Kafka acknowledges — at-least-once semantics.
     */
    private void commitOffset(KafkaConsumer<String, String> consumer,
                               ConsumerRecord<String, String> record) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(
            new TopicPartition(record.topic(), record.partition()),
            new OffsetAndMetadata(record.offset() + 1)
        );
        consumer.commitSync(offsets);
        log.debug("Committed offset partition={} offset={}",
                record.partition(), record.offset() + 1);
    }

    // ── SSE push ──────────────────────────────────────────────────────────────

    /**
     * Pushes {"status":"STORED|DLQ","event":{...}} to all connected browser clients.
     * Dead emitters (browser tab closed) are collected and removed after each push.
     */
    private void pushToSse(TelemetryEvent event, String status) {
        if (emitters.isEmpty()) return;
        String payload;
        try {
            Map<String, Object> envelope = new HashMap<>();
            envelope.put("status", status);
            envelope.put("event", event);
            payload = JsonUtil.toJson(envelope);
        } catch (Exception e) {
            log.warn("Failed to serialise SSE payload: {}", e.getMessage());
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
            log.debug("SLOW_MODE: sleeping {}ms", sleepMs);
            Thread.sleep(sleepMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ── Metrics ───────────────────────────────────────────────────────────────

    private void logMetrics() {
        if (processedCount.get() % 10 == 0) {
            log.info("[METRICS] processed={} failures={} store_size={}",
                    processedCount.get(), failureCount.get(), eventStore.size());
        }
    }

    // ── Public API for StorageController ─────────────────────────────────────

    public Map<String, TelemetryEvent> getEventStore() {
        return Collections.unmodifiableMap(eventStore);
    }

    public Map<String, Object> getMetrics() {
        Map<String, Object> m = new HashMap<>();
        m.put("processed", processedCount.get());
        m.put("failures",  failureCount.get());
        m.put("storeSize", eventStore.size());
        return m;
    }

    /** Called by StorageController to register a new browser SSE connection. */
    public SseEmitter subscribe() {
        SseEmitter emitter = new SseEmitter(0L); // 0 = no timeout
        emitters.add(emitter);
        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(()    -> emitters.remove(emitter));
        emitter.onError(e       -> emitters.remove(emitter));
        log.info("New SSE subscriber (storage) — active: {}", emitters.size());
        return emitter;
    }

    // ── Consumer properties ───────────────────────────────────────────────────

    private Properties buildConsumerProps() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,         AppConfig.BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,                  AppConfig.GROUP_STORAGE);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,    StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,  StringDeserializer.class.getName());
        // Start from the earliest uncommitted offset; useful for replaying history on restart
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,         "earliest");
        // Manual commit: offset advances only after successful business logic
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,        "false");
        return props;
    }

    // ── Inner exception type ──────────────────────────────────────────────────

    /** Distinguishes a deliberate simulated failure from unexpected runtime errors. */
    static class SimulatedFailureException extends RuntimeException {
        SimulatedFailureException(String msg) { super(msg); }
    }
}
