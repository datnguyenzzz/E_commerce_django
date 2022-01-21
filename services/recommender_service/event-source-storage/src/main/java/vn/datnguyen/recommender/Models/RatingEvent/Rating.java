package vn.datnguyen.recommender.Models.RatingEvent;

import vn.datnguyen.recommender.Models.PayloadEntity;

public class Rating extends PayloadEntity {
    private String clientId; 

    public Rating() {
        super();
    }

    public Rating(String clientId) {
        this.clientId = clientId;
    }

    public String getClientId() {
        return this.clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

}
