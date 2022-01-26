package vn.datnguyen.recommender.Handlers;

import java.util.List;
import java.util.Optional;

import vn.datnguyen.recommender.Models.Rating;

public interface QueryServices {
    Optional<Rating> findRatingById(String id);
    List<Rating> findAllRating();
}
