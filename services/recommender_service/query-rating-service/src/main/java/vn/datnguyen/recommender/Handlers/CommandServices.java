package vn.datnguyen.recommender.Handlers;

import vn.datnguyen.recommender.Models.Rating;

public interface CommandServices {
    void addNewClientRating(String clientId, String itemId, int score);
    void addNewClientRating(Rating rating);
    void updateClientRating(String clientId, String itemId, int score);
    void deleteClientRating(String clientId, String itemId);
    void deleteClientRating(Rating rating);
}
