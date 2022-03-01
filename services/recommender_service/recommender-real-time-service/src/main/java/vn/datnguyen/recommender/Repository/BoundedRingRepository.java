package vn.datnguyen.recommender.Repository;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;

public class BoundedRingRepository implements BoundedRingInterface {
    private final static String BOUNDED_RING_ROW = "bounded_ring_row";
    private final static String RING_ID = "ring_id";
    private final static String CENTRE_ID = "centre_id";
    private final static String LOWER_BOUND_RANGE = "lower_bound_range";
    private final static String UPPER_BOUND_RANGE = "upper_bound_range";

    public SimpleStatement createRowIfNotExists() {
        return SchemaBuilder.createTable(BOUNDED_RING_ROW)
            .ifNotExists()
            .withPartitionKey(RING_ID, DataTypes.INT)
            .withPartitionKey(CENTRE_ID, DataTypes.INT)
            .withColumn(LOWER_BOUND_RANGE, DataTypes.DOUBLE)
            .withColumn(UPPER_BOUND_RANGE, DataTypes.DOUBLE)
            .build();
    }
}
