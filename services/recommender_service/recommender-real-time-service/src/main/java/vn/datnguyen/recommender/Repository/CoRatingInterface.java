package vn.datnguyen.recommender.Repository;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;

public interface CoRatingInterface {
    SimpleStatement createRowIfNotExists();
    SimpleStatement createIndexOnItemId();
    SimpleStatement createIndexOnClientId();
    SimpleStatement findByItem1Id(String item1Id);
    SimpleStatement insertByItem1Id(String item1Id, String clientId, int newRating);
    SimpleStatement insertByItem2Id(String item2Id, String clientId, int newRating);
}
