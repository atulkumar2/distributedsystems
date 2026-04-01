package com.example.telematics.dlqviewer;

import com.example.telematics.model.DlqEvent;

public class DlqViewerRecord {

    private final long sequence;
    private final int partition;
    private final long offset;
    private final DlqEvent event;

    public DlqViewerRecord(long sequence, int partition, long offset, DlqEvent event) {
        this.sequence = sequence;
        this.partition = partition;
        this.offset = offset;
        this.event = event;
    }

    public long getSequence() { return sequence; }
    public int getPartition() { return partition; }
    public long getOffset() { return offset; }
    public DlqEvent getEvent() { return event; }
}
