package vn.datnguyen.recommender.Repository;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;

public interface PairCountInterface {
    SimpleStatement createRowIfNotExists(); 
    SimpleStatement findDeltaCoRating(String clientId, String itemId);
    SimpleStatement updateScore(String item1Id, String item2Id, int deltaScore);
}
