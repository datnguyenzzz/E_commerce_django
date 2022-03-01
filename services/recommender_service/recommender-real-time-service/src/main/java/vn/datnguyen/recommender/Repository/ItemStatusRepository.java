package vn.datnguyen.recommender.Repository;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;

public class ItemStatusRepository implements ItemStatusInterface {
    private final static String ITEM_STATUS_ROW = "item_status_row";
    private final static String ITEM_ID = "item_id";
    private final static String ADD_BY_CLIENT_ID = "add_by_client_id";
    private final static String BOUNDED_RING_ID = "bounded_ring_id";
    private final static String CENTRE_ID = "centre_id";

    public SimpleStatement createRowIfNotExists() {
        return SchemaBuilder.createTable(ITEM_STATUS_ROW)
            .ifNotExists()
            .withPartitionKey(ITEM_ID, DataTypes.TEXT)
            .withColumn(ADD_BY_CLIENT_ID, DataTypes.TEXT)
            .withColumn(BOUNDED_RING_ID, DataTypes.INT)
            .withColumn(CENTRE_ID, DataTypes.INT)
            .build();
    }
}
