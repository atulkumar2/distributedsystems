package com.example.telematics.model;

import java.time.Instant;

/**
 * Wraps a failed TelemetryEvent for the Dead-Letter Queue (DLQ).
 *
 * When the Storage Consumer detects an invalid event or a processing failure
 * it serialises a DlqEvent and publishes it to the `vehicle-telemetry-dlq` topic
 * so that the bad record is preserved for inspection without blocking offset commits.
 */
public class DlqEvent {

    private TelemetryEvent originalEvent;
    private String error;       // human-readable reason for rejection
    private String timestamp;   // ISO-8601 — when the failure occurred

    public DlqEvent() {}

    public DlqEvent(TelemetryEvent originalEvent, String error) {
        this.originalEvent = originalEvent;
        this.error         = error;
        this.timestamp     = Instant.now().toString();
    }

    // ── Getters / Setters ─────────────────────────────────────────────────────

    public TelemetryEvent getOriginalEvent()              { return originalEvent; }
    public void           setOriginalEvent(TelemetryEvent e) { this.originalEvent = e; }

    public String getError()                              { return error; }
    public void   setError(String e)                      { this.error = e; }

    public String getTimestamp()                          { return timestamp; }
    public void   setTimestamp(String t)                  { this.timestamp = t; }
}
