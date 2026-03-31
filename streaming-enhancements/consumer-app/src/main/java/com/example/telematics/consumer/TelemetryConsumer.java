package com.example.telematics.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
public class TelemetryConsumer {

    private static final Logger log = LoggerFactory.getLogger(TelemetryConsumer.class);

    // Thread-safe list of active SSE connections; multiple consumer worker threads write here
    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();

    /**
     * Called by ConsumerController when a browser opens /api/events/stream.
     * Returns an emitter that will receive every subsequent Kafka message.
     */
    public SseEmitter subscribe() {
        // 0L = no timeout; the browser reconnects automatically if the connection drops
        SseEmitter emitter = new SseEmitter(0L);
        emitters.add(emitter);
        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(() -> emitters.remove(emitter));
        emitter.onError(e -> emitters.remove(emitter));
        log.info("New SSE subscriber — total active: {}", emitters.size());
        return emitter;
    }

    /**
     * Called by ConsumerThreadPool workers for every Kafka record they process.
     * Pushes the raw JSON string to all currently connected SSE clients.
     * Safe to call from multiple threads concurrently.
     */
    public void push(String message) {
        List<SseEmitter> dead = new ArrayList<>();
        for (SseEmitter emitter : emitters) {
            try {
                // Send the raw JSON; the browser parses it via JSON.parse()
                emitter.send(SseEmitter.event().data(message));
            } catch (IOException e) {
                dead.add(emitter);
            }
        }
        emitters.removeAll(dead);
    }
}
