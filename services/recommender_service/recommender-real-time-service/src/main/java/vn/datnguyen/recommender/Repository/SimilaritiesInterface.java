package vn.datnguyen.recommender.Repository;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;

public interface SimilaritiesInterface {
    SimpleStatement createTableIfNotExists();
    SimpleStatement initScore(String item1Id, String item2Id, double score);
    SimpleStatement findBy(String item1Id, String item2Id);
    SimpleStatement findByItem1Id(String item1Id);
    SimpleStatement findByItem2Id(String item2Id);
    SimpleStatement findAllItemId();
    SimpleStatement updateScore(String item1Id, String item2Id, double newScore);
}
