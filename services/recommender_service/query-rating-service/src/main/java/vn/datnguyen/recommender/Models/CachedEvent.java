package vn.datnguyen.recommender.Models;

import java.io.Serializable;

import org.springframework.data.annotation.Id;
import org.springframework.data.redis.core.RedisHash;

@RedisHash(value = "Event", timeToLive = 300)
public class CachedEvent implements Serializable {

    private String eventId;
    private String eventType;

    public CachedEvent(String eventId, String eventType) {
        this.eventId = eventId;
        this.eventType = eventType;
    }

    @Id
    public String getEventId() {
        return this.eventId;
    }

    public String getEventType() {
        return this.eventType;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }
}
