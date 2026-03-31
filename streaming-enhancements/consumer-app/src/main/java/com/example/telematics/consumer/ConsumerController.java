package com.example.telematics.consumer;

import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Map;

@RestController
public class ConsumerController {

    private final TelemetryConsumer telemetryConsumer;
    private final ConsumerThreadPool consumerThreadPool;

    public ConsumerController(TelemetryConsumer telemetryConsumer,
                               ConsumerThreadPool consumerThreadPool) {
        this.telemetryConsumer  = telemetryConsumer;
        this.consumerThreadPool = consumerThreadPool;
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

    /**
     * Returns the current pool config and a snapshot of every worker's state.
     * Polled by the UI every 1.5 s to display the live thread dashboard.
     */
    @GetMapping("/api/consumers/status")
    public ResponseEntity<Map<String, Object>> getStatus() {
        return ResponseEntity.ok(consumerThreadPool.getStatus());
    }

    /**
     * Apply a new consumer pool configuration.
     * Body fields (all optional — omitted fields keep their current value):
     *   threadCount         int  1–10  (restarts the pool)
     *   processingDelayMs   int  0–5000
     *   crashEnabled        bool
     *   crashingThreadCount int  0–threadCount  (restarts the pool)
     */
    @PostMapping("/api/consumers/config")
    public ResponseEntity<Map<String, Object>> configure(@RequestBody Map<String, Object> body) {
        int     threadCount         = ((Number) body.getOrDefault("threadCount",         consumerThreadPool.threadCount)).intValue();
        int     processingDelayMs   = ((Number) body.getOrDefault("processingDelayMs",   consumerThreadPool.processingDelayMs)).intValue();
        boolean crashEnabled        = Boolean.TRUE.equals(body.getOrDefault("crashEnabled", consumerThreadPool.crashEnabled));
        int     crashingThreadCount = ((Number) body.getOrDefault("crashingThreadCount", consumerThreadPool.crashingThreadCount)).intValue();

        consumerThreadPool.configure(threadCount, processingDelayMs, crashEnabled, crashingThreadCount);
        return ResponseEntity.ok(consumerThreadPool.getStatus());
    }
}
