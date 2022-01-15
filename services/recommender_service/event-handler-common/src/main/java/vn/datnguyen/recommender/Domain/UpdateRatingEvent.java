package vn.datnguyen.recommender.Domain;

import java.util.Locale;

public class UpdateRatingEvent extends Event {
    private final String clientId; 
    private final String itemId; 
    private final int score;

    public UpdateRatingEvent(final String clientId, final String itemId, final int score) {
        super();
        this.clientId = clientId; 
        this.itemId = itemId; 
        this.score = score;
    }

    public String getClientId() {
        return this.clientId;
    }

    public String getItemId() {
        return this.itemId;
    }

    public int getScore() {
        return this.score;
    }

    @Override
    public String toString() {
        return String.format(Locale.getDefault(), 
                "UpdateRatingEvent{eventId=%s, timestamp=%d, clientId=%s, itemId=%s, score=%d}",
                this.getEventId(), this.getTimestamp(), this.clientId, this.itemId, this.score);
    }
}