package vn.datnguyen.recommender.Repository;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;

public interface BoundedRingInterface {
    public final static String BOUNDED_RING_ROW = "bounded_ring_row";
    public final static String RING_ID = "ring_id";
    public final static String CENTRE_ID = "centre_id";
    public final static String LOWER_BOUND_RANGE = "lower_bound_range";
    public final static String UPPER_BOUND_RANGE = "upper_bound_range";
    public final static String CAPACITY = "capacity";

    SimpleStatement createRowIfNotExists();
    SimpleStatement addNewBoundedRing(int ringId, int centreId, double lbRange, double ubRange);
    SimpleStatement findBoundedRingById(int ringId, int centreId);
    SimpleStatement updateBoundedRingCapacityById(int ringId, int centreId, int capacity);
}
