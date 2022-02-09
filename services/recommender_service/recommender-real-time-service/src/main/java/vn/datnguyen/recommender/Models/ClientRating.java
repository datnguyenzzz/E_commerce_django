package vn.datnguyen.recommender.Models;

public class ClientRating {
    private String clientId, itemId;
    private int rating;

    public ClientRating(String clientId, String itemId, int rating) {
        this.clientId = clientId;
        this.itemId = itemId;
        this.rating = rating;
    }

    public String getClientId() {
        return this.clientId;
    }

    public String getItemId() {
        return this.itemId;
    }

    public int getRating() {
        return this.rating;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    public void setRating(int rating) {
        this.rating = rating;
    }
}
