package com.example.telematics.producer;

import com.example.telematics.model.TelemetryEvent;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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

    /** Returns the full list of known vehicle IDs (sourced from vehicles.txt). */
    @GetMapping("/api/vehicles")
    public ResponseEntity<List<String>> getVehicles() {
        return ResponseEntity.ok(TelemetryEvent.getVehicleIds());
    }

    /** Generate and send a random event from the first vehicleCount vehicles (0 = all). */
    @PostMapping("/api/events/random")
    public ResponseEntity<TelemetryEvent> sendRandom(
            @RequestParam(defaultValue = "0") int vehicleCount) {
        TelemetryEvent event = TelemetryEvent.randomFrom(vehicleSubset(vehicleCount));
        producer.send(event);
        return ResponseEntity.ok(event);
    }

    /**
     * Send N random events using a fixed thread pool.
     * vehicleCount limits which vehicle IDs are sampled (0 = all vehicles).
     * fireAndForgetPct (0–100): percentage of events dispatched fire-and-forget.
     *   Those tasks are submitted to the pool but their futures are not tracked —
     *   they keep running in the background after the HTTP response is returned.
     *   The remaining (100 - pct)% are tracked; the endpoint awaits their futures
     *   and includes the sent events in the response body.
     */
    @PostMapping("/api/events/burst")
    public ResponseEntity<Map<String, Object>> sendBurst(
            @RequestParam(defaultValue = "10") int count,
            @RequestParam(defaultValue = "1")  int threads,
            @RequestParam(defaultValue = "0")  int vehicleCount,
            @RequestParam(defaultValue = "0")  int fireAndForgetPct) throws InterruptedException {
        count            = Math.min(Math.max(count,            1), 2000);
        threads          = Math.min(Math.max(threads,          1), 16);
        fireAndForgetPct = Math.min(Math.max(fireAndForgetPct, 0), 100);
        List<String> subset = vehicleSubset(vehicleCount);

        int fnfCount  = (int) Math.round(count * fireAndForgetPct / 100.0);
        int waitCount = count - fnfCount;

        ExecutorService pool = Executors.newFixedThreadPool(threads);

        // Tracked tasks — futures are collected so we wait only for these before responding.
        List<Future<?>> waitFutures = new ArrayList<>(waitCount);
        List<TelemetryEvent> events = Collections.synchronizedList(new ArrayList<>(waitCount));
        for (int i = 0; i < waitCount; i++) {
            Future<?> f = pool.submit(() -> {
                TelemetryEvent event = TelemetryEvent.randomFrom(subset);
                producer.send(event);
                events.add(event);
            });
            waitFutures.add(f);
        }

        // Fire-and-forget tasks — submitted to the same pool but futures intentionally
        // not tracked; these threads keep running after the HTTP response is returned.
        for (int i = 0; i < fnfCount; i++) {
            pool.submit(() -> producer.send(TelemetryEvent.randomFrom(subset)));
        }

        // Stop accepting new tasks; already-queued tasks continue unaffected.
        pool.shutdown();

        // Wait only for tracked futures — F&F futures are deliberately ignored.
        for (Future<?> f : waitFutures) {
            try { f.get(30, TimeUnit.SECONDS); } catch (Exception ignored) {}
        }

        return ResponseEntity.ok(Map.of(
            "sent",          events.size(),
            "fireAndForget", fnfCount,
            "threads",       threads,
            "events",        events
        ));
    }

    /**
     * Returns a randomly-sampled subset of vehicleCount IDs drawn from the full list,
     * or the full list when vehicleCount <= 0 or >= list size.
     * Sampling without replacement ensures no duplicates in the active subset.
     */
    private List<String> vehicleSubset(int vehicleCount) {
        List<String> all = TelemetryEvent.getVehicleIds();
        if (vehicleCount <= 0 || vehicleCount >= all.size()) {
            return all;
        }
        List<String> shuffled = new ArrayList<>(all);
        java.util.Collections.shuffle(shuffled);
        return shuffled.subList(0, vehicleCount);
    }
}
