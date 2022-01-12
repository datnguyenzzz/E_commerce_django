package vn.datnguyen.recommender.Backend;

import io.eventuate.ReflectiveMutableCommandProcessingAggregate;

public class Rating extends ReflectiveMutableCommandProcessingAggregate<Rating, RatingCommand> {
    private boolean isDeleted;
}
