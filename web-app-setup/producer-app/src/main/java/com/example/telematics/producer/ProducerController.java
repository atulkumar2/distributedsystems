package com.example.telematics.producer;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.Map;

@RestController
public class ProducerController {

    private final TelemetryProducer producer;

    public ProducerController(TelemetryProducer producer) {
        this.producer = producer;
    }

    /** Send a manually constructed event from the form. */
    @PostMapping("/api/events")
    public ResponseEntity<Map<String, Object>> sendEvent(@RequestBody TelemetryEvent event) {
        // Fill in server-side timestamp if the client didn't provide one
        if (event.getTimestamp() == null || event.getTimestamp().isBlank()) {
            event.setTimestamp(Instant.now().toString());
        }
        producer.send(event);
        return ResponseEntity.ok(Map.of(
            "status", "sent",
            "vehicleId", event.getVehicleId(),
            "timestamp", event.getTimestamp()
        ));
    }

    /** Generate and send a random event — useful for quick load testing. */
    @PostMapping("/api/events/random")
    public ResponseEntity<TelemetryEvent> sendRandom() {
        TelemetryEvent event = TelemetryEvent.random();
        producer.send(event);
        return ResponseEntity.ok(event);
    }
}
