package com.example.telematics.producer;

import com.example.telematics.model.TelemetryEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class TelemetryProducer {

    private static final Logger log = LoggerFactory.getLogger(TelemetryProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String topic;

    public TelemetryProducer(KafkaTemplate<String, String> kafkaTemplate,
                              ObjectMapper objectMapper,
                              @Value("${telemetry.topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.topic = topic;
    }

    public void send(TelemetryEvent event) {
        try {
            event.ensureEventId();
            String json = objectMapper.writeValueAsString(event);
            // Use vehicleId as the message key — preserves per-vehicle ordering
            kafkaTemplate.send(topic, event.getVehicleId(), json)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to send event for {}: {}", event.getVehicleId(), ex.getMessage());
                    } else {
                        log.info("Sent eventId={} key={} partition={} offset={}",
                            event.getEventId(),
                            event.getVehicleId(),
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                    }
                });
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialise TelemetryEvent", e);
        }
    }
}
