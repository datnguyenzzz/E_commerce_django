package vn.datnguyen.recommender.Repository;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;

public interface CoRatingInterface {
    SimpleStatement createRowIfNotExists();
    SimpleStatement createIndexes();
    SimpleStatement findByItem1Id(String item1Id);
}
