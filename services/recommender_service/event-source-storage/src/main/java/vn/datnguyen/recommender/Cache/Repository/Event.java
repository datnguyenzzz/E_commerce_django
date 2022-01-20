package vn.datnguyen.recommender.Cache.Repository;

import java.io.Serializable;

import org.springframework.data.redis.core.RedisHash;

@RedisHash("Event")
public class Event implements Serializable {
    private String eventId; 
    private boolean isHappened; 

    public Event(String eventId, boolean isHappened) {
        this.eventId = eventId;
        this.isHappened = isHappened;
    }
}
