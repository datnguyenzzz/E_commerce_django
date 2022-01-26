package vn.datnguyen.recommender.Handlers;

public interface CommandServices {
    void addNewClientRating(String clientId, String itemId, int score);
    void updateClientRating(String clientId, String itemId, int score);
    void deleteClientRating(String clientId, String itemId);
}
