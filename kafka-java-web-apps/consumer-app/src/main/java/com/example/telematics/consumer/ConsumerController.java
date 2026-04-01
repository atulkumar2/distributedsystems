package com.example.telematics.consumer;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@RestController
public class ConsumerController {

    private final TelemetryConsumer telemetryConsumer;

    public ConsumerController(TelemetryConsumer telemetryConsumer) {
        this.telemetryConsumer = telemetryConsumer;
    }

    /**
     * SSE endpoint — the browser connects here once and receives a stream of
     * newline-delimited JSON events as Kafka records arrive.
     * EventSource in the browser automatically reconnects on drop.
     */
    @GetMapping(value = "/api/events/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream() {
        return telemetryConsumer.subscribe();
    }
}
