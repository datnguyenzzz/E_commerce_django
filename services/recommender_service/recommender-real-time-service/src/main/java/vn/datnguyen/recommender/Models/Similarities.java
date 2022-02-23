package vn.datnguyen.recommender.Models;

public class Similarities {
    private String item1Id, item2Id; 
    private float scoreItemCount, scorePairCount; 

    public Similarities(String item1Id, String item2Id, float scoreItemCount, float scorePairCount) {
        this.item1Id = item1Id;
        this.item2Id = item2Id;
        this.scoreItemCount = scoreItemCount;
        this.scorePairCount = scorePairCount;
    }

    public String getItem1Id() {
        return this.item1Id;
    }

    public String getItem2Id() {
        return this.item2Id;
    }

    public float getScoreItemCount() {
        return this.scoreItemCount;
    }

    public float getScorePairCount() {
        return this.scorePairCount;
    }
}
