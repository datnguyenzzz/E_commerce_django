package vn.datnguyen.recommender.Models;

public class Similarities {
    private String item1Id, item2Id; 
    private float score; 

    public Similarities(String item1Id, String item2Id, float score) {
        this.item1Id = item1Id;
        this.item2Id = item2Id;
        this.score = score;
    }

    public String getItem1Id() {
        return this.item1Id;
    }

    public String getItem2Id() {
        return this.item2Id;
    }

    public float getScore() {
        return this.score;
    }
}
