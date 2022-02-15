package vn.datnguyen.recommender.Models;

public class Similarities {
    private String item1Id, item2Id; 
    private int pairCountScore, item1CountScore, item2CountScore; 
    private float score; 

    public Similarities(String item1Id, String item2Id, int pairCountScore, int item1CountScore, int item2CountScore, float score) {
        this.item1Id = item1Id;
        this.item2Id = item2Id;
        this.pairCountScore = pairCountScore; 
        this.item1CountScore = item1CountScore;
        this.item2CountScore = item2CountScore;
        this.score = score;
    }

    public String getItem1Id() {
        return this.item1Id;
    }

    public String getItem2Id() {
        return this.item2Id;
    }

    public int getPairCount() {
        return this.pairCountScore;
    }

    public int getItem1Count() {
        return this.item1CountScore;
    }

    public int getItem2Count() {
        return this.item2CountScore;
    }

    public float getScore() {
        return this.score;
    }
}
