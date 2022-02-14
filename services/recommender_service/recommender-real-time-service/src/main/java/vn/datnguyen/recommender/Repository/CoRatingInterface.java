package vn.datnguyen.recommender.Repository;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;

public interface CoRatingInterface {
    SimpleStatement createRowIfNotExists();
    SimpleStatement createIndexOnItemId();
    SimpleStatement createIndexOnClientId();
    SimpleStatement findByItem1IdAndClientId(String item1Id, String clientId);
    SimpleStatement updateItem1Score(String itemId, String clientId, int newRating, int deltaRating);
    SimpleStatement updateItem2Score(String itemId, String clientId, int newRating, int deltaRating);
}
