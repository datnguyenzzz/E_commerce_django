package vn.datnguyen.recommender.Backend;

import io.eventuate.ReflectiveMutableCommandProcessingAggregate;

//@Service
public class Rating extends ReflectiveMutableCommandProcessingAggregate<Rating, RatingCommand> {
    private boolean isDeleted;
}
