package vn.datnguyen.recommender.Models;

import java.util.List;

public class Event {
    private String eventId, timestamp, eventType, clientId, itemId; 
    private int weight, limit;
    private List<Integer> coord;

    public Event() {}

    public Event(String eventId, String timestamp, String eventType, String clientId, String itemId, int weight, int limit, List<Integer> coord) {
        this.eventId = eventId;
        this.timestamp = timestamp;
        this.eventType = eventType; 
        this.clientId = clientId; 
        this.itemId = itemId; 
        this.weight = weight;
        this.limit = limit;
        this.coord = coord;
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

    public int getLimit() {
        return limit;
    }

    public List<Integer> getCoord() {
        return this.coord;
    }
}