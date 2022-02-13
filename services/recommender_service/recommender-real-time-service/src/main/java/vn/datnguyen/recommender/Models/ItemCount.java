package vn.datnguyen.recommender.Models;

public class ItemCount {
    private String itemId;
    private int score;

    public ItemCount(String itemId, int score) {
        this.itemId = itemId;
        this.score = score;
    }

    public String getItemId() {
        return itemId;
    }

    public int getScore() {
        return score;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    public void setScore(int score) {
        this.score = score;
    }
}
