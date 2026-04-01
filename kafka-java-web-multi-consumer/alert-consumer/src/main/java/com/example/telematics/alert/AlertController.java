package com.example.telematics.alert;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Map;

@RestController
public class AlertController {

    private final AlertService alertService;

    public AlertController(AlertService alertService) {
        this.alertService = alertService;
    }

    /** Returns current alert metrics: processed, alerts, warnings. */
    @GetMapping("/api/metrics")
    public Map<String, Object> getMetrics() {
        return alertService.getMetrics();
    }

    /**
     * SSE stream — browsers receive {"type":"ALERT|WARNING|OK","vehicleId":"...","speed":N,"fuelLevel":N}
     * for every evaluated Kafka record.  EventSource in the browser reconnects automatically.
     */
    @GetMapping(value = "/api/alerts/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream() {
        return alertService.subscribe();
    }
}
