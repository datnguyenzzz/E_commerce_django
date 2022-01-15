package vn.datnguyen.recommender.Domain;

import java.util.UUID;

abstract public class Event {
    private final String eventId; 
    private final long timestamp;
    private final int partitionId;

    public Event() {
        this(UUID.randomUUID().toString(), System.currentTimeMillis(), 0);
    }

    public Event(final int partitionId) {
        this(UUID.randomUUID().toString(), System.currentTimeMillis(), partitionId);
    }

    public Event(final String eventId, final long timestamp, final int partitionId) {
        this.eventId = eventId; 
        this.timestamp = timestamp;
        this.partitionId = partitionId;
    }

    public String getEventId() {
        return this.eventId;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public int getPartitionId() {
        return this.partitionId;
    }
}
