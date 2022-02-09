package vn.datnguyen.recommender.Models;
    
public class Event {
    private String eventId, timestamp, eventType, clientId, itemId; 
    private int weight;

    public Event() {}

    public Event(String eventId, String timestamp, String eventType, String clientId, String itemId, int weight) {
        this.eventId = eventId;
        this.timestamp = timestamp;
        this.eventType = eventType; 
        this.clientId = clientId; 
        this.itemId = itemId; 
        this.weight = weight;
    }

    public String getEventId() {
        return eventId;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public String getEventType() {
        return eventType; 
    }

    public String getClientId() {
        return clientId;
    }

    public String getItemId() {
        return itemId;
    }

    public int getWeight() {
        return weight;
    }
}