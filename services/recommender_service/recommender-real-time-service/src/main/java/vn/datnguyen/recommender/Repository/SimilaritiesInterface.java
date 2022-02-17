package vn.datnguyen.recommender.Repository;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;

public interface SimilaritiesInterface {
    SimpleStatement createTableIfNotExists();
}
