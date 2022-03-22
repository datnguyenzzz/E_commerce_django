package vn.datnguyen.recommender.Models;

public class RecommendedItem {
    private String itemId;
    private double distance;

    public RecommendedItem(String itemId, double distance) {
        this.itemId = itemId; 
        this.distance = distance;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    public void setDistance(double distance) {
        this.distance = distance;
    }

    public String getItemId() {
        return this.itemId;
    }

    public double getDistance() {
        return this.distance;
    }

    @Override
    public String toString() {
        return "\n\t\t[ ItemId = " + this.itemId + ", diff = " + this.distance + " ]\n";
    }
}

