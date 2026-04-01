package com.example.telematics.dlqviewer;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.List;
import java.util.Map;

@RestController
public class DlqViewerController {

    private final DlqViewerService service;

    public DlqViewerController(DlqViewerService service) {
        this.service = service;
    }

    @GetMapping("/api/dlq")
    public List<DlqViewerRecord> getRecentDlqRecords() {
        return service.getRecentRecords();
    }

    @GetMapping("/api/metrics")
    public Map<String, Object> getMetrics() {
        return service.getMetrics();
    }

    @GetMapping(value = "/api/dlq/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream() {
        return service.subscribe();
    }
}
