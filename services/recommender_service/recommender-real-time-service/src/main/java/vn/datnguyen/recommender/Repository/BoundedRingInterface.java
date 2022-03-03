package vn.datnguyen.recommender.Repository;

import java.util.UUID;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;

public interface BoundedRingInterface {
    public final static String BOUNDED_RING_ROW = "bounded_ring_row";
    public final static String RING_ID = "ring_id"; // UUID
    public final static String CENTRE_ID = "centre_id"; //int
    public final static String LOWER_BOUND_RANGE = "lower_bound_range"; //double
    public final static String UPPER_BOUND_RANGE = "upper_bound_range"; // double
    public final static String CAPACITY = "capacity"; // int

    SimpleStatement createRowIfNotExists();
    SimpleStatement addNewBoundedRing(UUID ringId, int centreId, double lbRange, double ubRange);
    SimpleStatement findBoundedRingById(UUID ringId, int centreId);
    SimpleStatement findAllBoundedRingInCentre(int centreId);
    SimpleStatement updateBoundedRingCapacityById(UUID ringId, int centreId, int capacity);
    SimpleStatement updateBoundedRingRange(UUID ringId, int centreId, double lbRange, double ubRange);
}
