package vn.datnguyen.recommender.Repository;

import java.util.List;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;

public interface IndexesCoordInterface {
    SimpleStatement createRowIfNotExists();
    SimpleStatement insertNewIndex(int id, List<Integer> coord);
    SimpleStatement selectAllCentre();
}
