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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class StorageService implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(StorageService.class);

    private final Map<String, TelemetryEvent> eventStore = new ConcurrentHashMap<>();
    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();
    private final AtomicLong processedCount = new AtomicLong(0);
    private final AtomicLong failureCount = new AtomicLong(0);
    private final AtomicLong duplicateCount = new AtomicLong(0);
    private final Random random = new Random();
    private final TelemetryEventRepository repository;

    public StorageService(TelemetryEventRepository repository) {
        this.repository = repository;
    }

    @Override
    public void run(ApplicationArguments args) {
        eventStore.clear();
        eventStore.putAll(repository.loadLatestByVehicle());
        log.info("Hydrated storage cache from Postgres — latest_vehicle_count={}", eventStore.size());

        Thread pollThread = new Thread(this::pollLoop, "storage-kafka-poll");
        pollThread.setDaemon(true);
        pollThread.start();
        log.info("Storage Kafka poll thread started — group={} topic={}",
                AppConfig.GROUP_STORAGE, AppConfig.TOPIC_TELEMETRY);
    }

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

    private void handleRecord(ConsumerRecord<String, String> record,
                              KafkaConsumer<String, String> consumer,
                              DlqProducer dlqProducer) {
        applySlowMode();

        TelemetryEvent event = null;
        String eventId = null;
        try {
            event = JsonUtil.fromJson(record.value(), TelemetryEvent.class);
            eventId = ensureEventIdentity(event, record);

            log.info("[eventId={}][vehicleId={}][partition={}][offset={}] Received speed={} fuel={}",
                    eventId, event.getVehicleId(), record.partition(), record.offset(),
                    event.getSpeed(), event.getFuelLevel());

            processWithRetry(event, eventId, record, consumer, dlqProducer);
        } catch (Exception e) {
            failureCount.incrementAndGet();
            log.error("[partition={}][offset={}] Malformed JSON — routing to DLQ: {}",
                    record.partition(), record.offset(), e.getMessage());

            if (event == null) {
                event = buildFallbackEvent(record);
            }
            dlqProducer.send(event, "Malformed JSON: " + e.getMessage());
            pushToSse(event, "DLQ");
            commitOffset(consumer, record);
        }
    }

    private void processWithRetry(TelemetryEvent event, String eventId,
                                  ConsumerRecord<String, String> record,
                                  KafkaConsumer<String, String> consumer,
                                  DlqProducer dlqProducer) {
        int attempt = 0;
        Exception lastException = null;

        while (attempt < AppConfig.MAX_RETRIES) {
            attempt++;
            try {
                boolean inserted = process(event, eventId, record);
                commitOffset(consumer, record);
                if (inserted) {
                    processedCount.incrementAndGet();
                    pushToSse(event, "STORED");
                } else {
                    duplicateCount.incrementAndGet();
                }
                logMetrics();
                return;
            } catch (SimulatedFailureException sfe) {
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

        failureCount.incrementAndGet();
        log.error("[eventId={}][vehicleId={}] All {} retries exhausted — routing to DLQ",
                eventId, event.getVehicleId(), AppConfig.MAX_RETRIES);
        dlqProducer.send(event, "Failed after " + AppConfig.MAX_RETRIES + " retries: "
                + (lastException != null ? lastException.getMessage() : "unknown"));
        pushToSse(event, "DLQ");
        commitOffset(consumer, record);
    }

    private boolean process(TelemetryEvent event, String eventId, ConsumerRecord<String, String> record) {
        if (event.getSpeed() > AppConfig.SPEED_FAILURE_THRESHOLD) {
            throw new SimulatedFailureException(
                    "Simulated failure: speed=" + event.getSpeed()
                            + " exceeds threshold=" + AppConfig.SPEED_FAILURE_THRESHOLD);
        }

        boolean inserted = repository.insert(event, record.partition(), record.offset());
        if (!inserted) {
            log.info("[eventId={}][vehicleId={}][partition={}][offset={}] Duplicate insert ignored",
                    eventId, event.getVehicleId(), record.partition(), record.offset());
            return false;
        }

        eventStore.put(event.getVehicleId(), event);
        log.info("[eventId={}][vehicleId={}][partition={}][offset={}] Stored in Postgres — latest_vehicle_count={}",
                eventId, event.getVehicleId(), record.partition(), record.offset(), eventStore.size());
        return true;
    }

    private String ensureEventIdentity(TelemetryEvent event, ConsumerRecord<String, String> record) {
        if (event.getTimestamp() == null || event.getTimestamp().isBlank()) {
            event.setTimestamp(Instant.now().toString());
        }
        if (event.getEventId() == null || event.getEventId().isBlank()) {
            event.setEventId(record.topic() + "-" + record.partition() + "-" + record.offset());
        }
        return event.getEventId();
    }

    private TelemetryEvent buildFallbackEvent(ConsumerRecord<String, String> record) {
        TelemetryEvent event = new TelemetryEvent();
        event.setEventId(record.topic() + "-" + record.partition() + "-" + record.offset());
        event.setVehicleId(record.key() != null ? record.key() : "unknown");
        event.setTimestamp("1970-01-01T00:00:00Z");
        return event;
    }

    private void commitOffset(KafkaConsumer<String, String> consumer, ConsumerRecord<String, String> record) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(
                new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset() + 1)
        );
        consumer.commitSync(offsets);
        log.debug("Committed offset partition={} offset={}",
                record.partition(), record.offset() + 1);
    }

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

    private void logMetrics() {
        if ((processedCount.get() + duplicateCount.get()) % 10 == 0) {
            log.info("[METRICS] processed={} duplicates={} failures={} store_size={}",
                    processedCount.get(), duplicateCount.get(), failureCount.get(), eventStore.size());
        }
    }

    public Map<String, TelemetryEvent> getEventStore() {
        return Collections.unmodifiableMap(eventStore);
    }

    public Map<String, Object> getMetrics() {
        Map<String, Object> m = new HashMap<>();
        m.put("processed", processedCount.get());
        m.put("duplicates", duplicateCount.get());
        m.put("failures", failureCount.get());
        m.put("storeSize", eventStore.size());
        return m;
    }

    public SseEmitter subscribe() {
        SseEmitter emitter = new SseEmitter(0L);
        emitters.add(emitter);
        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(() -> emitters.remove(emitter));
        emitter.onError(e -> emitters.remove(emitter));
        log.info("New SSE subscriber (storage) — active: {}", emitters.size());
        return emitter;
    }

    private Properties buildConsumerProps() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, AppConfig.GROUP_STORAGE);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "storage-consumer-1");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return props;
    }

    static class SimulatedFailureException extends RuntimeException {
        SimulatedFailureException(String msg) { super(msg); }
    }
}
