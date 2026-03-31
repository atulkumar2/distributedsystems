package com.example.telematics.producer;

import com.example.telematics.model.TelemetryEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simulates a single vehicle's realistic driving behaviour as a background thread.
 *
 * <p>State machine transitions happen once per tick (1 s). The vehicle autonomously
 * moves through: CRUISING → ACCELERATING / DECELERATING → STOPPED_AT_INTERSECTION,
 * with occasional hard events (SUDDEN_BRAKE, SUDDEN_ACCELERATION).
 *
 * <p>User controls (called from HTTP threads):
 * <ul>
 *   <li><b>pause</b>  – thread sleeps, no events emitted</li>
 *   <li><b>resume</b> – wakes a paused vehicle</li>
 *   <li><b>freeze</b> – state machine locked; events still emitted with current values</li>
 *   <li><b>unfreeze</b> – re-enables state transitions</li>
 *   <li><b>kill</b>   – terminal; thread exits on next tick</li>
 * </ul>
 */
public class VehicleSimulator implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(VehicleSimulator.class);

    /** All visible vehicle states (including user-controlled ones). */
    public enum State {
        STARTING,
        CRUISING,
        ACCELERATING,
        DECELERATING,
        SUDDEN_BRAKE,
        SUDDEN_ACCELERATION,
        STOPPED,
        STOPPED_AT_INTERSECTION,
        PAUSED,
        FROZEN,
        KILLED
    }

    // ── Identity ───────────────────────────────────────────────────────────────
    private final String vehicleId;
    private final TelemetryProducer producer;
    private final Random rng = new Random();

    // ── Simulation state (volatile: written by sim thread, read by status endpoint) ──
    volatile State  state        = State.STARTING;
    volatile double speed        = 0;
    volatile double fuelLevel    = 60 + new Random().nextDouble() * 40; // 60–100 %
    volatile double latitude     = 12.90 + new Random().nextDouble() * 0.20;  // Bangalore bbox
    volatile double longitude    = 77.50 + new Random().nextDouble() * 0.20;
    volatile String engineStatus = "ON";

    // ── Internal state-machine helpers ─────────────────────────────────────────
    private double targetSpeed = 50 + rng.nextDouble() * 50; // km/h
    private int    waitTicks   = 0;                           // countdown while stopped

    // ── Control flags (written by HTTP threads, read by sim thread) ────────────
    volatile boolean pauseRequested = false;
    volatile boolean frozen         = false;
    volatile boolean killed         = false;

    // ── Stats ──────────────────────────────────────────────────────────────────
    private final AtomicLong eventCount = new AtomicLong(0);
    volatile String lastEventAt = "";

    public VehicleSimulator(String vehicleId, TelemetryProducer producer) {
        this.vehicleId = vehicleId;
        this.producer  = producer;
    }

    // ── Runnable ───────────────────────────────────────────────────────────────

    @Override
    public void run() {
        // Randomise starting speed so vehicles don't all begin at 0
        speed = 30 + rng.nextDouble() * 50;
        state = State.CRUISING;
        log.info("[always-on] {} started", vehicleId);
        try {
            while (!killed) {
                handlePauseIfRequested();
                if (killed) break;
                if (!frozen) evolveState();
                if (killed) break;   // don't send an event if killed during evolveState
                sendEvent();
                killAwareSleep(1_000);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        state = State.KILLED;
        log.info("[always-on] {} stopped", vehicleId);
    }

    /** Block inside a tight sleep loop while pause is requested. */
    private void handlePauseIfRequested() throws InterruptedException {
        if (!pauseRequested) return;
        state = State.PAUSED;
        while (pauseRequested && !killed) Thread.sleep(100);
        if (!killed) state = State.CRUISING;
    }

    /**
     * Sleep for {@code millis} total, but wake up within ~100 ms when
     * {@code killed} is set — without requiring Thread.interrupt().
     * Also responds immediately to Thread.interrupt() (InterruptedException propagates).
     */
    private void killAwareSleep(long millis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + millis;
        while (!killed && System.currentTimeMillis() < deadline) {
            Thread.sleep(100);
        }
    }

    // ── State-machine tick ─────────────────────────────────────────────────────

    private void evolveState() {
        switch (state) {
            case CRUISING                -> tickCruising();
            case ACCELERATING            -> tickAccelerating();
            case DECELERATING            -> tickDecelerating();
            case SUDDEN_BRAKE            -> tickSuddenBrake();
            case SUDDEN_ACCELERATION     -> tickSuddenAcceleration();
            case STOPPED                 -> tickStopped(false);
            case STOPPED_AT_INTERSECTION -> tickStopped(true);
            default -> { /* no-op for other states */ }
        }

        // Fuel drain: base idle + speed-proportional consumption
        fuelLevel = Math.max(0, fuelLevel - 0.005 - speed / 12_000.0);

        // Slow position drift proportional to speed (approx 1 km ≈ 0.009°)
        double drift = speed / 3_600.0 * 0.009;
        latitude  += drift * (rng.nextDouble() - 0.5) * 2;
        longitude += drift * (rng.nextDouble() - 0.5) * 2;
    }

    private void tickCruising() {
        // Gently drift speed toward target with small noise
        speed += (targetSpeed - speed) * 0.1 + rng.nextGaussian() * 1.5;
        speed  = Math.max(0, speed);
        engineStatus = "ON";

        // Transition dice-roll (out of 100)
        int roll = rng.nextInt(100);
        if      (roll <  4) enterStopAtIntersection();
        else if (roll <  7) { state = State.SUDDEN_BRAKE; }
        else if (roll < 10) { state = State.SUDDEN_ACCELERATION; targetSpeed = 90 + rng.nextDouble() * 40; }
        else if (roll < 22) { state = State.ACCELERATING;        targetSpeed = 50 + rng.nextDouble() * 80; }
        else if (roll < 34) { state = State.DECELERATING;        targetSpeed = Math.max(0, speed - 20 - rng.nextDouble() * 30); }
        // else: stay CRUISING
    }

    private void tickAccelerating() {
        speed = Math.min(speed + 8 + rng.nextDouble() * 7, 160);
        if (speed >= targetSpeed) { speed = targetSpeed; state = State.CRUISING; }
    }

    private void tickDecelerating() {
        speed = Math.max(speed - 6 - rng.nextDouble() * 6, 0);
        if (speed <= Math.max(targetSpeed, 0) + 3) {
            if (speed < 5) { speed = 0; enterStop(4 + rng.nextInt(6)); }
            else           { state = State.CRUISING; }
        }
    }

    private void tickSuddenBrake() {
        // Hard stop: lose 35–55 km/h per tick
        speed = Math.max(speed - 35 - rng.nextDouble() * 20, 0);
        if (speed <= 0) { speed = 0; enterStop(2 + rng.nextInt(5)); }
        else            { state = State.CRUISING; }
    }

    private void tickSuddenAcceleration() {
        // Stomp the pedal: gain 25–45 km/h in one tick then cruise
        speed = Math.min(speed + 25 + rng.nextDouble() * 20, 160);
        state = State.CRUISING;
    }

    private void tickStopped(boolean atIntersection) {
        speed        = 0;
        engineStatus = "IDLE";
        if (--waitTicks <= 0) {
            state        = State.ACCELERATING;
            targetSpeed  = atIntersection
                    ? 30 + rng.nextDouble() * 40
                    : 40 + rng.nextDouble() * 60;
            engineStatus = "ON";
        }
    }

    private void enterStop(int ticks) {
        state        = State.STOPPED;
        waitTicks    = ticks;
        engineStatus = "IDLE";
    }

    private void enterStopAtIntersection() {
        state        = State.STOPPED_AT_INTERSECTION;
        waitTicks    = 6 + rng.nextInt(12); // wait 6–18 ticks at the light
        speed        = 0;
        engineStatus = "IDLE";
    }

    // ── Event dispatch ─────────────────────────────────────────────────────────

    private void sendEvent() {
        TelemetryEvent ev = new TelemetryEvent(
                vehicleId,
                Instant.now().toString(),
                round6(latitude),
                round6(longitude),
                Math.max(0, round1(speed)),
                Math.max(0, round1(fuelLevel)),
                engineStatus
        );
        producer.send(ev);
        lastEventAt = ev.getTimestamp();
        eventCount.incrementAndGet();
    }

    // ── User control (called from HTTP threads) ────────────────────────────────

    /** Pause sending — vehicle thread sleeps, no events emitted. */
    public void pause()    { pauseRequested = true; frozen = false; }

    /** Wake a paused vehicle. */
    public void resume()   { pauseRequested = false; }

    /** Lock state machine — events still sent but no behaviour changes. */
    public void freeze()   {
        frozen = true;
        pauseRequested = false;
        if (state != State.PAUSED) state = State.FROZEN;
    }

    /** Re-enable behaviour changes. */
    public void unfreeze() { frozen = false; if (state == State.FROZEN) state = State.CRUISING; }

    /** Terminate the simulation loop on the next tick. */
    public void kill()     { killed = true; pauseRequested = false; frozen = false; }

    public boolean isKilled() { return killed; }

    // ── Status snapshot (safe to call from any thread) ──────────────────────────

    public Map<String, Object> getStatus() {
        return Map.of(
                "vehicleId",    vehicleId,
                "state",        state.name(),
                "speed",        Math.max(0, round1(speed)),
                "fuelLevel",    Math.max(0, round1(fuelLevel)),
                "engineStatus", engineStatus,
                "eventCount",   eventCount.get(),
                "lastEventAt",  lastEventAt,
                "frozen",       frozen,
                "paused",       pauseRequested
        );
    }

    public String getVehicleId() { return vehicleId; }

    // ── Helpers ────────────────────────────────────────────────────────────────

    private static double round1(double v) { return Math.round(v * 10d) / 10d; }
    private static double round6(double v) { return Math.round(v * 1_000_000d) / 1_000_000d; }
}
