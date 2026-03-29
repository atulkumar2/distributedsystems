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

public class KafkaConsumerExample {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerExample.class);
    private static final String TOPIC = "vehicle-telemetry";

    public static void main(String[] args) {
        Properties properties = new Properties();

        // Broker endpoint comes from config file so local port can be changed without code edits.
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.getBootstrapServers());
        // Consumer group means one logical application with one shared offset state.
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "vehicle-telemetry-group");
        // Deserializers read key/value bytes back into Java String objects.
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 'earliest' is useful in learning: if no committed offset exists, start from beginning.
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(TOPIC));
            log.info("Consumer subscribed to topic '{}'. Press Ctrl+C to stop.", TOPIC);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    // Same key (vehicleId) tends to stay in one partition, preserving per-vehicle order.
                    log.info("Received key={} value={} partition={} offset={}",
                            record.key(), record.value(), record.partition(), record.offset());
                }
            }
        }
    }
}
