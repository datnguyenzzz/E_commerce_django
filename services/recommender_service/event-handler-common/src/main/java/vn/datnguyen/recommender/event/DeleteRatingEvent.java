package vn.datnguyen.recommender.event;

import java.util.Locale;

public class DeleteRatingEvent extends Event {
    private final String clientId; 
    private final String itemId; 

    public DeleteRatingEvent(final String clientId, final String itemId, final int score) {
        super();
        this.clientId = clientId; 
        this.itemId = itemId; 
    }

    public String getClientId() {
        return this.clientId;
    }

    public String getItemId() {
        return this.itemId;
    }

    @Override
    public String toString() {
        return String.format(Locale.getDefault(), 
                "DeleteRatingEvent{eventId=%s, clientId=%s, itemId=%s}",
                this.getEventId(), this.clientId, this.itemId);
    }
}

