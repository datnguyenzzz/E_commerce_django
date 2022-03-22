package vn.datnguyen.recommender.Models;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;

public class KnnResult {

    private String eventId;
    private List<RecommendedItem> recommendedItemList;

    public KnnResult(String eventId) {
        this.eventId = eventId;
        this.recommendedItemList = new ArrayList<>();
    }

    public void addToRecommendationList(ImmutablePair<Double, String> result) {
        RecommendedItem rItem = new RecommendedItem(result.getRight(), result.getLeft());
        this.recommendedItemList.add(rItem);
    }

    public String getEventId() {
        return this.eventId;
    }

    public List<RecommendedItem> getRecommendedItemList() {
        return this.recommendedItemList;
    }

    @Override
    public String toString() {
        return "Recommendation by KNN method for is"
                + " \n\teventId = " + eventId
                + " \n\trecommended items = " + recommendedItemList;
    }
}
