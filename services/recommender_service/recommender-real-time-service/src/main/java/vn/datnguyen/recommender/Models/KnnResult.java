package vn.datnguyen.recommender.Models;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;

public class KnnResult {

    class RecommendedItem {
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

        @Override
        public String toString() {
            return "[ ItemId = " + this.itemId + ", diff = " + this.distance + " ]";
        }
    }

    private String eventId;
    private List<Integer> eventCoord;
    private List<RecommendedItem> recommendedItemList;

    public KnnResult(String eventId, List<Integer> eventCoord) {
        this.eventId = eventId;
        this.eventCoord = eventCoord;
        this.recommendedItemList = new ArrayList<>();
    }

    public void addToRecommendationList(ImmutablePair<Double, String> result) {
        RecommendedItem rItem = new RecommendedItem(result.getRight(), result.getLeft());
        this.recommendedItemList.add(rItem);
    }

    public String getEventId() {
        return this.eventId;
    }

    public List<Integer> getEventCoord() {
        return this.eventCoord;
    }
}
