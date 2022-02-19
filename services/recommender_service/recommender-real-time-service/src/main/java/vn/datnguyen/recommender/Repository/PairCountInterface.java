package vn.datnguyen.recommender.Repository;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;

public interface PairCountInterface {
    SimpleStatement createRowIfNotExists(); 
    SimpleStatement updateScore(String item1Id, String item2Id, int deltaScore);
    SimpleStatement getCurrentScore(String item1Id, String item2Id);
    SimpleStatement initNewScore(String item1Id, String item2Id);
}
