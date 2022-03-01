package vn.datnguyen.recommender.Repository;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;

public interface ItemStatusInterface {
    SimpleStatement createRowIfNotExists();
}
