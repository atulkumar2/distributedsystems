package com.example.telematics.producer;

import com.example.telematics.model.TelemetryEvent;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@RestController
public class ProducerController {

    private final TelemetryProducer producer;
    private final AlwaysOnPool alwaysOnPool;

    public ProducerController(TelemetryProducer producer, AlwaysOnPool alwaysOnPool) {
        this.producer     = producer;
        this.alwaysOnPool = alwaysOnPool;
    }

    /** Send a manually constructed event from the form. */
    @PostMapping("/api/events")
    public ResponseEntity<Map<String, Object>> sendEvent(@RequestBody TelemetryEvent event) {
        // Fill in server-side timestamp if the client didn't provide one
        if (event.getTimestamp() == null || event.getTimestamp().isBlank()) {
            event.setTimestamp(Instant.now().toString());
        }
        event.ensureEventId();
        producer.send(event);
        return ResponseEntity.ok(Map.of(
            "status", "sent",
            "eventId", event.getEventId(),
            "vehicleId", event.getVehicleId(),
            "timestamp", event.getTimestamp()
        ));
    }

    /**
     * Returns vehicle IDs that are NOT reserved by the always-on pool.
     * The compose form and burst send panels both use this list.
     */
    @GetMapping("/api/vehicles")
    public ResponseEntity<List<String>> getVehicles() {
        Set<String> reserved = alwaysOnPool.getReservedIds();
        List<String> available = TelemetryEvent.getVehicleIds().stream()
                .filter(id -> !reserved.contains(id))
                .collect(Collectors.toList());
        return ResponseEntity.ok(available);
    }

    /** Generate and send a random event from available (non-reserved) vehicles. */
    @PostMapping("/api/events/random")
    public ResponseEntity<?> sendRandom(
            @RequestParam(defaultValue = "0") int vehicleCount) {
        List<String> subset = vehicleSubset(vehicleCount);
        if (subset.isEmpty()) {
            return ResponseEntity.status(503)
                    .body(Map.of("error", "No available vehicles — all reserved by always-on panel"));
        }
        TelemetryEvent event = TelemetryEvent.randomFrom(subset);
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

    // ── Always-On Vehicle Pool endpoints ─────────────────────────────────────

    /**
     * Reserve and start {@code count} random available vehicles in the always-on pool.
     * Returns the list of newly-activated vehicle IDs.
     */
    @PostMapping("/api/alwayson/activate")
    public ResponseEntity<Map<String, Object>> activateVehicles(
            @RequestParam(defaultValue = "5") int count) {
        List<String> started = alwaysOnPool.activate(count);
        return ResponseEntity.ok(Map.of(
                "activated", started.size(),
                "vehicles",  started
        ));
    }

    /** Current status snapshot for all always-on vehicles. */
    @GetMapping("/api/alwayson/status")
    public ResponseEntity<Map<String, Object>> alwaysOnStatus() {
        return ResponseEntity.ok(alwaysOnPool.getStatus());
    }

    /**
     * Control a single always-on vehicle.
     * {@code action}: pause | resume | freeze | unfreeze | kill | start
     */
    @PostMapping("/api/alwayson/{vehicleId}/control")
    public ResponseEntity<Map<String, Object>> controlVehicle(
            @PathVariable String vehicleId,
            @RequestParam String action) {
        boolean ok = alwaysOnPool.control(vehicleId, action);
        if (!ok) {
            return ResponseEntity.badRequest()
                    .body(Map.of("error", "Vehicle not reserved in always-on pool: " + vehicleId));
        }
        return ResponseEntity.ok(Map.of("vehicleId", vehicleId, "action", action));
    }

    /** Kill all always-on vehicles and free their IDs back to other panels. */
    @PostMapping("/api/alwayson/deactivate")
    public ResponseEntity<Map<String, Object>> deactivateAll() {
        alwaysOnPool.deactivateAll();
        return ResponseEntity.ok(Map.of("status", "all deactivated"));
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Returns a randomly-sampled subset of available (non-reserved) vehicle IDs.
     * vehicleCount=0 means "all available". Sampling without replacement prevents duplicates.
     */
    private List<String> vehicleSubset(int vehicleCount) {
        Set<String> reserved = alwaysOnPool.getReservedIds();
        List<String> all = TelemetryEvent.getVehicleIds().stream()
                .filter(id -> !reserved.contains(id))
                .collect(Collectors.toList());
        if (vehicleCount <= 0 || vehicleCount >= all.size()) {
            return all;
        }
        List<String> shuffled = new ArrayList<>(all);
        Collections.shuffle(shuffled);
        return shuffled.subList(0, vehicleCount);
    }
}
