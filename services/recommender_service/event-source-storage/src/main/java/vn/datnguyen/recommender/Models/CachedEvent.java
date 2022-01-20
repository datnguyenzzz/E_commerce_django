package vn.datnguyen.recommender.Models;

import java.io.Serializable;

import org.springframework.data.annotation.Id;
import org.springframework.data.redis.core.RedisHash;

@RedisHash(value = "Event", timeToLive = 300)
public class CachedEvent implements Serializable {
    @Id
    private String eventId; 
    private String eventType;

    public CachedEvent(String eventId, String eventType) {
        this.eventId = eventId;
        this.eventType = eventType;
    }

    public String getEventId() {
        return this.eventId;
    }

    public void setId(String eventId) {
        this.eventId = eventId;
    }

    public String getEventType() {
        return this.eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }


    @Override
    public String toString() {
        return "Event "  + getEventId() + " type = " + getEventType();
    }
}
