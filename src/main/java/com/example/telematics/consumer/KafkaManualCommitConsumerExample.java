package com.example.telematics.consumer;

import com.example.telematics.util.KafkaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaManualCommitConsumerExample {
    private static final Logger log = LoggerFactory.getLogger(KafkaManualCommitConsumerExample.class);
    private static final String TOPIC = "vehicle-telemetry";

    public static void main(String[] args) {
        Properties properties = new Properties();

        // Broker endpoint comes from config file so local port can be changed without code edits.
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "vehicle-telemetry-group-manual-commit");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Turning off auto commit gives us explicit control over when offsets are marked as done.
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(TOPIC));
            log.info("Manual commit consumer subscribed. Press Ctrl+C to stop.");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    // In real systems, processing might include DB writes, alerts, or enrichment.
                    // Commit should happen after that processing succeeds.
                    log.info("Processed key={} value={} partition={} offset={}",
                            record.key(), record.value(), record.partition(), record.offset());
                }

                if (!records.isEmpty()) {
                    // Manual commit reduces data loss risk: if process crashes before this call,
                    // records are re-read after restart instead of silently skipped.
                    consumer.commitSync();
                    log.info("Offsets committed manually for latest processed batch.");
                }
            }
        }
    }
}
