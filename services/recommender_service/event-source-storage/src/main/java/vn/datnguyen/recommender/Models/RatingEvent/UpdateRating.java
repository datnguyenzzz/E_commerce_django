package vn.datnguyen.recommender.Models.RatingEvent;

public class UpdateRating extends Rating {
    private String itemId; 
    private int score; 

    public UpdateRating() {
        super();
    }

    public UpdateRating(String clientId, String itemId, int score) {
        super(clientId);
        this.itemId = itemId; 
        this.score = score;
    }

    public String getItemId() {
        return this.itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    public int getScore() {
        return this.score;
    }

    public void setScore(int score) {
        this.score = score;
    }

    @Override
    public String toString() {
        return "Client-" + getClientId() + " update rating to item-" + getItemId() + " with score=" + getScore();
    }

}
