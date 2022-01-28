package vn.datnguyen.recommender.Handlers;

import java.util.List;

import vn.datnguyen.recommender.Models.Rating;

public interface RatingRepositoryCustom {
    List<Rating> findByClientId(String clientId);
    List<Rating> findByItemId(String itemId);
    List<Rating> findByClientIdAndItemId(String clientId, String itemId);
}
