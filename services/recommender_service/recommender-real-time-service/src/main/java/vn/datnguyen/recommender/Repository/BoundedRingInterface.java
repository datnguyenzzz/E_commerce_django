package vn.datnguyen.recommender.Repository;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.uuid.Uuids;

public interface BoundedRingInterface {
    public final static String BOUNDED_RING_ROW = "bounded_ring_row";
    public final static String RING_ID = "ring_id"; // UUID
    public final static String CENTRE_ID = "centre_id"; //int
    public final static String LOWER_BOUND_RANGE = "lower_bound_range"; //double
    public final static String UPPER_BOUND_RANGE = "upper_bound_range"; // double
    public final static String CAPACITY = "capacity"; // int

    SimpleStatement createRowIfNotExists();
    SimpleStatement addNewBoundedRing(Uuids ringId, int centreId, double lbRange, double ubRange);
    SimpleStatement findBoundedRingById(Uuids ringId, int centreId);
    SimpleStatement updateBoundedRingCapacityById(Uuids ringId, int centreId, int capacity);
}
