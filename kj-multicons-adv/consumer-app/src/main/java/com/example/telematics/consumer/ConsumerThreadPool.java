package com.example.telematics.consumer;

import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages a pool of raw KafkaConsumer threads.
 *
 * Why raw KafkaConsumer instead of @KafkaListener?
 * - Full lifecycle control: start/stop/crash individual threads at runtime
 * - Demonstrates what Spring Kafka hides: each consumer in a group gets a
 *   partition slice; adding more threads than partitions leaves some idle
 * - Crash simulation: when a thread throws, Kafka detects the missing heartbeat
 *   and triggers a consumer group rebalance — the surviving threads pick up
 *   the abandoned partitions
 *
 * All workers belong to the same consumer group, so Kafka distributes the
 * topic's partitions among them (max useful threads == partition count).
 */
@Component
public class ConsumerThreadPool implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(ConsumerThreadPool.class);

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value("${telemetry.topic}")
    private String topic;

    private final TelemetryConsumer telemetryConsumer;

    // ── Mutable config — written by HTTP thread, read by worker threads ─────
    // Workers check these on every message; changes take effect without restart
    // (except threadCount and crashingThreadCount, which require a pool restart).
    volatile int     threadCount        = 1;
    volatile int     processingDelayMs  = 0;
    volatile boolean crashEnabled       = false;
    volatile int     crashingThreadCount = 0;

    private final List<ConsumerWorker> workers = new CopyOnWriteArrayList<>();
    private final AtomicInteger idSeq = new AtomicInteger(0);

    public ConsumerThreadPool(TelemetryConsumer telemetryConsumer) {
        this.telemetryConsumer = telemetryConsumer;
    }

    @PostConstruct
    public void init() {
        launchWorkers(threadCount, crashingThreadCount);
    }

    /**
     * Apply a new configuration.  Stops the entire pool and restarts it so
     * thread count and crash assignments are applied cleanly.
     * processingDelayMs and crashEnabled are volatile — they take effect
     * immediately on currently running workers as well.
     */
    public synchronized void configure(int newThreadCount, int newDelayMs,
                                        boolean newCrashEnabled, int newCrashingThreadCount) {
        this.processingDelayMs    = Math.max(0, Math.min(newDelayMs, 5000));
        this.crashEnabled         = newCrashEnabled;
        this.threadCount          = Math.max(1, Math.min(newThreadCount, 10));
        this.crashingThreadCount  = Math.max(0, Math.min(newCrashingThreadCount, this.threadCount));

        stopAllWorkers();
        launchWorkers(this.threadCount, this.crashingThreadCount);

        log.info("Pool reconfigured: threads={} delayMs={} crashEnabled={} crashingThreadCount={}",
                 this.threadCount, this.processingDelayMs, this.crashEnabled, this.crashingThreadCount);
    }

    /** Returns a snapshot of the current config and every worker's state. */
    public Map<String, Object> getStatus() {
        List<Map<String, Object>> workerSnapshots = new ArrayList<>();
        for (ConsumerWorker w : workers) {
            workerSnapshots.add(Map.of(
                "id",                 w.id,
                "state",              w.state,
                "messagesProcessed",  w.messagesProcessed,
                "crashCount",         w.crashCount,
                "isCrashingThread",   w.isCrashingThread,
                "assignedPartitions", w.assignedPartitions,
                "lastCrashAt",        w.lastCrashAt
            ));
        }
        return Map.of(
            "config", Map.of(
                "threadCount",         threadCount,
                "processingDelayMs",   processingDelayMs,
                "crashEnabled",        crashEnabled,
                "crashingThreadCount", crashingThreadCount
            ),
            "workers", workerSnapshots
        );
    }

    private void launchWorkers(int count, int crashCount) {
        for (int i = 0; i < count; i++) {
            // The first crashCount workers are designated as crashing threads
            boolean willCrash = i < crashCount;
            ConsumerWorker w = new ConsumerWorker(idSeq.incrementAndGet(), willCrash);
            workers.add(w);
            Thread t = new Thread(w, "consumer-worker-" + w.id);
            t.setDaemon(true);
            t.start();
        }
    }

    private void stopAllWorkers() {
        // Snapshot the list, clear it immediately, then signal each worker to stop.
        // Workers that finish later call workers.remove(this) which is a harmless no-op.
        List<ConsumerWorker> current = new ArrayList<>(workers);
        workers.clear();
        for (ConsumerWorker w : current) {
            w.stop();
        }
    }

    @Override
    public void destroy() {
        stopAllWorkers();
    }

    // ── ConsumerWorker ────────────────────────────────────────────────────────

    /**
     * Each worker creates its own KafkaConsumer (same consumer group).
     * Kafka assigns this consumer a slice of the topic's partitions.
     * If there are more workers than partitions the extra workers stay IDLE.
     *
     * Crash simulation: when crashEnabled is true and isCrashingThread is true,
     * the worker throws after processing a random number of messages (10–40).
     * The exception closes the KafkaConsumer (triggering a rebalance) and the
     * worker sleeps for 2 s before reconnecting — mimicking a real consumer crash.
     */
    class ConsumerWorker implements Runnable {

        final int     id;
        final boolean isCrashingThread;

        volatile String state              = "STARTING";
        volatile int    messagesProcessed  = 0;
        volatile int    crashCount         = 0;
        volatile long   lastCrashAt        = 0;
        volatile int    assignedPartitions = 0;
        volatile boolean stopRequested     = false;

        private Thread thread;

        // Countdown to next simulated crash; reset after each crash
        private int messagesUntilCrash;

        ConsumerWorker(int id, boolean isCrashingThread) {
            this.id               = id;
            this.isCrashingThread = isCrashingThread;
            this.messagesUntilCrash = nextCrashInterval();
        }

        void stop() {
            stopRequested = true;
            if (thread != null) thread.interrupt();
        }

        /** Crash after processing between 10 and 40 messages. */
        private int nextCrashInterval() {
            return 10 + new Random().nextInt(31);
        }

        @Override
        public void run() {
            this.thread = Thread.currentThread();

            while (!stopRequested) {
                state = "STARTING";

                // Build a KafkaConsumer with raw Properties — same config as application.properties
                Properties props = new Properties();
                props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,       bootstrapServers);
                props.put(ConsumerConfig.GROUP_ID_CONFIG,                 groupId);
                // Fixed, sequential id — appears in broker logs as "consumer-worker-1", "consumer-worker-2", etc.
                // Makes it trivial to correlate a log line with a specific pool slot.
                props.put(ConsumerConfig.CLIENT_ID_CONFIG,                "consumer-worker-" + id);
                props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,        "earliest");
                props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,       "true");
                props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,         "20");
                // Short session timeout so Kafka detects a crashed worker quickly
                // and triggers a rebalance to redistribute its partitions.
                props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,       "10000");
                props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG,    "3000");

                try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
                    consumer.subscribe(List.of(topic));
                    state = "RUNNING";

                    while (!stopRequested) {
                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(300));

                        // Reflect partition assignment; workers that get no partitions are IDLE
                        assignedPartitions = consumer.assignment().size();
                        if (records.isEmpty()) {
                            if (assignedPartitions == 0) state = "IDLE";
                            continue;
                        }

                        for (ConsumerRecord<String, String> record : records) {
                            if (stopRequested) break;

                            state = "PROCESSING";

                            // Simulate slow consumer — useful for observing lag build-up in Kafka UI
                            if (processingDelayMs > 0) {
                                Thread.sleep(processingDelayMs);
                            }

                            // Push the raw JSON to all connected SSE clients
                            telemetryConsumer.push(record.value());
                            messagesProcessed++;

                            // Crash simulation: decrement the countdown; throw when it hits 0.
                            // This closes the KafkaConsumer, losing the heartbeat heartbeat so
                            // Kafka detects a dead consumer and redistributes its partitions.
                            if (crashEnabled && isCrashingThread) {
                                messagesUntilCrash--;
                                if (messagesUntilCrash <= 0) {
                                    messagesUntilCrash = nextCrashInterval();
                                    throw new RuntimeException(
                                        "Simulated crash on worker-" + id +
                                        " after " + messagesProcessed + " total messages");
                                }
                            }

                            state = "RUNNING";
                        }
                    }

                } catch (InterruptedException e) {
                    // Normal shutdown path — restore interrupted flag and exit
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    if (stopRequested) break;

                    // Genuine crash (may be simulated): record it, then restart after a pause
                    crashCount++;
                    lastCrashAt = System.currentTimeMillis();
                    state = "RESTARTING";
                    log.warn("Worker {} crashed (total crashes: {}) — restarting in 2 s: {}",
                             id, crashCount, e.getMessage());
                    try {
                        // The 2 s gap ensures Kafka's session timeout fires and a rebalance occurs
                        Thread.sleep(2000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }

            state = "STOPPED";
            workers.remove(this);
            log.info("Worker {} stopped", id);
        }
    }
}
