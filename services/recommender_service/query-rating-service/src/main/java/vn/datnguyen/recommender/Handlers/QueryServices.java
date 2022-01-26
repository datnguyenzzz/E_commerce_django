package vn.datnguyen.recommender.Handlers;

import vn.datnguyen.recommender.Models.Rating;

public interface QueryServices {
    
    void addNewRating(Rating rating);

    void deleteRating(Rating rating);
}
