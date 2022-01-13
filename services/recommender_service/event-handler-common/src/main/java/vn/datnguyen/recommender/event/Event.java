package vn.datnguyen.recommender.event;

import java.util.UUID;

abstract public class Event {
    private final String eventId; 
    private final long timestamp;

    public Event() {
        this(UUID.randomUUID().toString(), System.currentTimeMillis());
    }

    public Event(final String eventId, final long timestamp) {
        this.eventId = eventId; 
        this.timestamp = timestamp;
    }

    public String getEventId() {
        return this.eventId;
    }

    public long getTimestamp() {
        return this.timestamp;
    }
}
