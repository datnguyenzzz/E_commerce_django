package vn.datnguyen.recommender.Models.RatingEvent;

public class PublishRating extends Rating {
    private String itemId; 
    private int score; 

    public PublishRating() {
        super();
    }

    public PublishRating(String clientId, String itemId, int score) {
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
        return "Client-" + getClientId() + " publish rating to item-" + getItemId() + " with score=" + getScore();
    }

}
