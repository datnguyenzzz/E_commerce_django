package vn.datnguyen.recommender.Repository;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;

public interface CoRatingInterface {
    SimpleStatement createRowIfNotExists();
    SimpleStatement createIndexOnItemId();
    SimpleStatement createIndexOnClientId();
    //
    SimpleStatement findByItem1IdAndClientId(String item1Id, String clientId);
    SimpleStatement findByItem2IdAndClientId(String item1Id, String clientId);
    SimpleStatement findSetItemIdByClientId(String clientId);
    //
    SimpleStatement updateItemScore(String item1Id, String item2Id, String clientId, int newScore, int deltaScore);
    SimpleStatement updateItem1Rating(String item1Id, String item2Id, String clientId, int newRating);
    SimpleStatement updateItem2Rating(String itemId, String item2Id, String clientId, int newRating);
    //
    SimpleStatement insertNewItemScore(String item1Id, String item2Id, String clientId);
}
