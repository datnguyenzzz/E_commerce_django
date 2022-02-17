package vn.datnguyen.recommender.Repository;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;

public interface SimilaritiesInterface {
    SimpleStatement createTableIfNotExists();
    SimpleStatement initScore(String item1Id, String item2Id);
    SimpleStatement findBy(String item1Id, String item2Id);
    SimpleStatement updateItem1Count(String item1Id, float newScore);
    SimpleStatement updateItem2Count(String item2Id, float newScore);
    SimpleStatement updatePairCount(String item1Id, String item2Id, float newScore);
}
