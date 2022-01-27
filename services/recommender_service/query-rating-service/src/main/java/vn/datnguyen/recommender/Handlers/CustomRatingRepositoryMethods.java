package vn.datnguyen.recommender.Handlers;

import java.util.List;

import vn.datnguyen.recommender.Models.Rating;

public interface CustomRatingRepositoryMethods {
    // Command side
    // Query side
    List<Rating> findByClientId(String clientId);
    List<Rating> findByItemId(String itemId);
    Rating findByClientIdAndItemId(String clientId, String itemId);
}
