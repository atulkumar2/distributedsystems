package com.example.telematics.producer;

import com.example.telematics.model.TelemetryEvent;
import com.example.telematics.util.JsonUtil;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class KafkaProducerExample {
    private static final String TOPIC = "vehicle-telemetry";

    public static void main(String[] args) {
        Properties properties = new Properties();

        // Kafka broker endpoint for local development.
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // Key serializer is String because we use vehicleId as the message key.
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Value serializer is String because payload is JSON text.
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Leader + replicas must acknowledge before send is considered successful.
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        // Retries improve resilience for temporary network or broker issues.
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);
        // Small delay allows batching more records together for better throughput.
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 10);

        List<TelemetryEvent> sampleEvents = generateSampleEvents(10);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            for (TelemetryEvent event : sampleEvents) {
                String key = event.getVehicleId();
                String jsonPayload = JsonUtil.toJson(event);

                // Practical partitioning rule: same key is consistently mapped to the same partition.
                // Using vehicleId as key keeps one vehicle's event order stable for consumers.
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, jsonPayload);

                Callback callback = (RecordMetadata metadata, Exception exception) -> {
                    if (exception != null) {
                        System.err.println("Send failed for key=" + key + ": " + exception.getMessage());
                    } else {
                        System.out.printf(
                                "Sent key=%s to topic=%s partition=%d offset=%d%n",
                                key,
                                metadata.topic(),
                                metadata.partition(),
                                metadata.offset()
                        );
                    }
                };

                producer.send(record, callback);
            }

            producer.flush();
            System.out.println("Producer finished sending telemetry events.");
        }

        // Kafka helps IoT/microservices by decoupling data producers from multiple downstream consumers.
        // The producer only writes to Kafka; analytics, alerts, and storage services consume independently.
    }

    private static List<TelemetryEvent> generateSampleEvents(int count) {
        Random random = new Random();
        List<TelemetryEvent> events = new ArrayList<>();
        String[] vehicleIds = {"VH-1001", "VH-1002", "VH-1003"};

        for (int i = 0; i < count; i++) {
            String vehicleId = vehicleIds[i % vehicleIds.length];
            double latitude = 12.90 + random.nextDouble() * 0.20;
            double longitude = 77.50 + random.nextDouble() * 0.20;
            double speed = 30 + random.nextDouble() * 70;
            double fuelLevel = 20 + random.nextDouble() * 80;
            String engineStatus = speed > 0 ? "ON" : "OFF";

            events.add(new TelemetryEvent(
                    vehicleId,
                    Instant.now().toString(),
                    round(latitude),
                    round(longitude),
                    round(speed),
                    round(fuelLevel),
                    engineStatus
            ));
        }

        return events;
    }

    private static double round(double value) {
        return Math.round(value * 100.0) / 100.0;
    }
}
