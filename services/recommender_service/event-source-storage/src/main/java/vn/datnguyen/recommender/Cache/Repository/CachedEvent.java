package vn.datnguyen.recommender.Cache.Repository;

import java.io.Serializable;

import org.springframework.data.redis.core.RedisHash;

@RedisHash(value = "Event", timeToLive = 300)
public class CachedEvent implements Serializable {
    private String id; 
    private String eventType;

    public CachedEvent(String id, String eventType) {
        this.id = id;
        this.eventType = eventType;
    }

    public String getId() {
        return this.id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getEventType() {
        return this.eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }


    @Override
    public String toString() {
        return "Event "  + getId() + " type = " + getEventType();
    }
}
