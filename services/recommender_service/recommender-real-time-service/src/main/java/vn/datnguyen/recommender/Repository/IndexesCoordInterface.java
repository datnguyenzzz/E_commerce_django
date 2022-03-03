package vn.datnguyen.recommender.Repository;

import java.util.List;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;

public interface IndexesCoordInterface {
    public final static String INDEXES_COORD_ROW = "indexes_coord_row";
    public final static String CENTRE_ID = "centre_id";
    public final static String CENTRE_COORD = "centre_coord";
    public final static String CENTRE_UPPER_BOUND_RANGE_LIST = "centre_upper_bound_range_list";

    SimpleStatement createRowIfNotExists();
    SimpleStatement insertNewIndex(int id, List<Integer> coord);
    SimpleStatement selectAllCentre();
    SimpleStatement selectCentreById(int id);
    //
    SimpleStatement updateUBRangeListById(int id, List<Double> ubRangeList);
}
