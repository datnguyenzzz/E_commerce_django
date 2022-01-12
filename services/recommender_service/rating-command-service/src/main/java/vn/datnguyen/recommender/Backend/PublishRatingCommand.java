package vn.datnguyen.recommender.Backend;

public class PublishRatingCommand implements RatingCommand {
    private final int clientId;
    private final int itemId;
    private final int score; 

    public PublishRatingCommand(int clientId, int itemId, int score) {
        this.clientId = clientId;
        this.itemId = itemId;
        this.score = score;
    }

    public int getScore() {
        return this.score;
    }

    public int getClientId() {
        return this.clientId;
    }

    public int getItemid() {
        return this.itemId;
    }
}
