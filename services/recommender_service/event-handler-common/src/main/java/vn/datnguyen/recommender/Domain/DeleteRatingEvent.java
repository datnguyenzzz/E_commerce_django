package vn.datnguyen.recommender.Domain;

import java.util.Locale;

public class DeleteRatingEvent extends Event {
    private final String clientId; 
    private final String itemId; 

    public DeleteRatingEvent(final String clientId, final String itemId) {
        super();
        this.clientId = clientId; 
        this.itemId = itemId; 
    }

    public DeleteRatingEvent(final String clientId, final String itemId, int partitionId) {
        super(partitionId);
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
                "DeleteRatingEvent{eventId=%s, timestamp=%d, clientId=%s, itemId=%s}",
                this.getEventId(), this.getTimestamp(), this.clientId, this.itemId);
    }
}

