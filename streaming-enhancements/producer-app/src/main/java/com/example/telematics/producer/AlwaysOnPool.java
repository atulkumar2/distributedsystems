package com.example.telematics.producer;

import com.example.telematics.model.TelemetryEvent;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Spring service that manages the "always-on" vehicle simulator pool.
 *
 * <p>Vehicles reserved here are excluded from burst / random / compose send so that
 * their telemetry is not mixed with manually-triggered data.
 *
 * <p>Design notes:
 * <ul>
 *   <li>Killed vehicles remain in {@code simulators} so they still appear in the UI
 *       and their IDs stay reserved — visible but inactive.</li>
 *   <li>The pool is fully cleared only via {@link #deactivateAll()}, which frees
 *       every reserved ID back to the other panels.</li>
 *   <li>Worker threads are Java virtual threads (Java 21+), so large numbers of
 *       concurrent vehicles carry negligible OS overhead.</li>
 * </ul>
 */
@Service
public class AlwaysOnPool {

    private final TelemetryProducer producer;

    // vehicleId → simulator (includes KILLED entries to keep reserved-ID set stable)
    private final Map<String, VehicleSimulator> simulators = new ConcurrentHashMap<>();

    // vehicleId → thread (removed on kill so GC can collect the thread)
    private final Map<String, Thread> threads = new ConcurrentHashMap<>();

    public AlwaysOnPool(TelemetryProducer producer) {
        this.producer = producer;
    }

    // ── Activation / deactivation ──────────────────────────────────────────────

    /**
     * Ensure exactly {@code count} non-killed vehicles are running in the pool.
     *
     * <p>Calling this method is idempotent: if the pool already has {@code count}
     * or more active (non-killed) vehicles, no new simulators are started and an
     * empty list is returned.  If the pool has fewer, only the missing vehicles
     * are started so the total active count reaches {@code count}.
     *
     * <p>This prevents the double-activate bug where clicking Activate again
     * with the same count would double the pool size.
     */
    public synchronized List<String> activate(int count) {
        count = Math.min(Math.max(count, 1), 100);

        // Count how many vehicles are currently alive (not killed)
        long activeNow = simulators.values().stream()
                .filter(s -> !s.isKilled())
                .count();
        int toAdd = count - (int) activeNow;
        if (toAdd <= 0) return List.of();  // already at or above target — no-op

        List<String> available = new ArrayList<>(TelemetryEvent.getVehicleIds());
        available.removeAll(simulators.keySet()); // exclude already-reserved vehicles
        if (available.isEmpty()) return List.of();

        Collections.shuffle(available);
        int toStart = Math.min(toAdd, available.size());

        List<String> started = new ArrayList<>(toStart);
        for (int i = 0; i < toStart; i++) {
            String id = available.get(i);
            launchSimulator(id);
            started.add(id);
        }
        return started;
    }

    /**
     * Kill all simulators and clear the entire pool, freeing every reserved ID
     * back to the other panels immediately.
     */
    public synchronized void deactivateAll() {
        simulators.values().forEach(VehicleSimulator::kill);
        threads.values().forEach(Thread::interrupt);
        simulators.clear();
        threads.clear();
    }

    // ── Per-vehicle control ────────────────────────────────────────────────────

    /**
     * Control a specific vehicle.
     *
     * @param vehicleId the target vehicle
     * @param action    one of: {@code pause | resume | freeze | unfreeze | kill | start}
     * @return {@code false} if the vehicle ID is not reserved in this pool
     */
    public boolean control(String vehicleId, String action) {
        VehicleSimulator sim = simulators.get(vehicleId);
        if (sim == null) return false;

        switch (action.toLowerCase(Locale.ROOT)) {
            case "pause"    -> sim.pause();
            case "resume"   -> sim.resume();
            case "freeze"   -> sim.freeze();
            case "unfreeze" -> sim.unfreeze();
            case "kill"     -> {
                sim.kill();
                Thread t = threads.remove(vehicleId);
                if (t != null) t.interrupt();
                // Keep entry in simulators so the box stays in the UI as KILLED
            }
            case "start"    -> {
                // Re-launch only if actually in KILLED state (idempotent otherwise)
                if (sim.isKilled()) {
                    simulators.remove(vehicleId);
                    launchSimulator(vehicleId);
                }
            }
            default -> { return false; }
        }
        return true;
    }

    // ── Status / query API ─────────────────────────────────────────────────────

    /**
     * All vehicle IDs currently reserved (running, paused, frozen, or killed).
     * Used by {@link ProducerController} to exclude these from burst/random send.
     */
    public Set<String> getReservedIds() {
        return simulators.keySet();
    }

    /** Full status snapshot for the {@code GET /api/alwayson/status} endpoint. */
    public Map<String, Object> getStatus() {
        List<Map<String, Object>> vehicles = simulators.values().stream()
                .map(VehicleSimulator::getStatus)
                .sorted(Comparator.comparing(m -> (String) m.get("vehicleId")))
                .collect(Collectors.toList());
        return Map.of(
                "activeCount", simulators.size(),
                "vehicles",    vehicles
        );
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    private void launchSimulator(String vehicleId) {
        VehicleSimulator sim = new VehicleSimulator(vehicleId, producer);
        // Virtual threads — one per vehicle, negligible OS cost
        Thread t = Thread.ofVirtual()
                .name("vehicle-sim-" + vehicleId)
                .start(sim);
        simulators.put(vehicleId, sim);
        threads.put(vehicleId, t);
    }

    @PreDestroy
    public void shutdown() {
        deactivateAll();
    }
}
