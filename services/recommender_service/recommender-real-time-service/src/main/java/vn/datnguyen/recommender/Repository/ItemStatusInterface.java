package vn.datnguyen.recommender.Repository;

import java.util.List;
import java.util.UUID;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;

public interface ItemStatusInterface {

    public final static String ITEM_STATUS_ROW = "item_status_row";
    public final static String ITEM_ID = "item_id"; // string
    public final static String ADD_BY_CLIENT_ID = "add_by_client_id"; // string
    public final static String BOUNDED_RING_ID = "bounded_ring_id"; // UUID
    public final static String CENTRE_ID = "centre_id"; //int
    public final static String DISTANCE_TO_CENTRE = "distance_to_centre"; //double
    public final static String VECTOR_PROPERTIES = "vector_properties"; //list<int>

    SimpleStatement createRowIfNotExists();
    SimpleStatement addNewItemStatus(String itemId, String clientId, UUID boundedRingId, int centreId, double dist, List<Integer> properties);
    SimpleStatement findAllByRingId(UUID boundedRingId, int centreId);
    SimpleStatement deleteItemStatus(String itemId, UUID boundedRingId, int centreId);
}
