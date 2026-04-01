package com.example.telematics.dlq;

import com.example.telematics.config.AppConfig;
import com.example.telematics.model.DlqEvent;
import com.example.telematics.model.TelemetryEvent;
import com.example.telematics.util.JsonUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Thin Kafka producer dedicated to the Dead-Letter Queue topic.
 *
 * Why a dedicated DLQ producer?
 *  - Keeps failure-path code isolated from the happy path.
 *  - Consumers can share a single instance of this class (thread-safe).
 *  - Makes it easy to add DLQ-specific config (e.g. different acks, retries) later.
 *
 * Usage:
 *   DlqProducer dlq = new DlqProducer();
 *   dlq.send(event, "Invalid speed value");
 *   dlq.close();   // call once on shutdown
 */
public class DlqProducer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DlqProducer.class);

    private final KafkaProducer<String, String> producer;

    public DlqProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // acks=all: wait for all in-sync replicas to acknowledge — maximises durability
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        // Retry up to 3 times on transient network issues before giving up
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        this.producer = new KafkaProducer<>(props);
    }

    /**
     * Publishes a failed event to the DLQ topic.
     *
     * @param original  the TelemetryEvent that could not be processed
     * @param reason    human-readable description of why processing failed
     */
    public void send(TelemetryEvent original, String reason) {
        DlqEvent dlqEvent = new DlqEvent(original, reason);
        String json = JsonUtil.toJson(dlqEvent);

        // Use vehicleId as the key so DLQ records land on the same partition as
        // the original message — preserving per-vehicle ordering in the DLQ as well.
        ProducerRecord<String, String> record =
                new ProducerRecord<>(AppConfig.TOPIC_DLQ, original.getVehicleId(), json);

        producer.send(record, (meta, ex) -> {
            if (ex != null) {
                log.error("[DLQ] Failed to publish to DLQ for vehicleId={} reason={}: {}",
                        original.getVehicleId(), reason, ex.getMessage());
            } else {
                log.warn("[DLQ] Event sent to DLQ topic={} vehicleId={} partition={} offset={} reason={}",
                        AppConfig.TOPIC_DLQ,
                        original.getVehicleId(),
                        meta.partition(),
                        meta.offset(),
                        reason);
            }
        });
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }
}
