package vn.datnguyen.recommender.Backend;

import io.eventuate.ReflectiveMutableCommandProcessingAggregate;

public class Rating extends ReflectiveMutableCommandProcessingAggregate<Rating, RatingCommand> {
    private int clientId; 
    private int itemId;
    private int score; 
    private boolean isDelete;

    public int getScore() {
        return this.score;
    }
}
