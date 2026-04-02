package com.example.telematics.storage;

import com.example.telematics.model.TelemetryEvent;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Map;

@RestController
public class StorageController {

    private final StorageService storageService;

    public StorageController(StorageService storageService) {
        this.storageService = storageService;
    }

    /** Returns the latest persisted event per vehicle, keyed by vehicleId. */
    @GetMapping("/api/store")
    public Map<String, TelemetryEvent> getStore() {
        return storageService.getEventStore();
    }

    /** Returns processing metrics: processed, duplicates, failures, storeSize. */
    @GetMapping("/api/metrics")
    public Map<String, Object> getMetrics() {
        return storageService.getMetrics();
    }

    /**
     * SSE stream — each browser EventSource connection receives a push notification
     * every time a Kafka record is processed.  Payload: {"status":"STORED|DLQ","event":{...}}
     */
    @GetMapping(value = "/api/events/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream() {
        return storageService.subscribe();
    }
}
