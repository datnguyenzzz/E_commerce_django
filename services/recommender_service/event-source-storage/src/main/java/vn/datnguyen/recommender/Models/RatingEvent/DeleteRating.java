package vn.datnguyen.recommender.Models.RatingEvent;

public class DeleteRating extends Rating {
    private String itemId; 

    public DeleteRating() {
        super();
    }

    public DeleteRating(String clientId, String itemId) {
        super(clientId);
        this.itemId = itemId; 
    }

    public String getItemId() {
        return this.itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    @Override
    public String toString() {
        return "Client-" + getClientId() + " delete rating to item-" + getItemId() ;
    }

}
