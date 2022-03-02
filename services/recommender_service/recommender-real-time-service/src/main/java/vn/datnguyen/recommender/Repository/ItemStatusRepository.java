package vn.datnguyen.recommender.Repository;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;

public class ItemStatusRepository implements ItemStatusInterface {

    public SimpleStatement createRowIfNotExists() {
        return SchemaBuilder.createTable(ITEM_STATUS_ROW)
            .ifNotExists()
            .withPartitionKey(ITEM_ID, DataTypes.TEXT)
            .withColumn(ADD_BY_CLIENT_ID, DataTypes.TEXT)
            .withColumn(BOUNDED_RING_ID, DataTypes.INT)
            .withColumn(CENTRE_ID, DataTypes.INT)
            .build();
    }

    public SimpleStatement addNewItemStatus(String itemId, String clientId, int boundedRingId, int centreId) {
        return QueryBuilder.insertInto(ITEM_STATUS_ROW)
            .value(ITEM_ID, QueryBuilder.literal(itemId))
            .value(ADD_BY_CLIENT_ID, QueryBuilder.literal(clientId))
            .value(BOUNDED_RING_ID, QueryBuilder.literal(boundedRingId))
            .value(CENTRE_ID, QueryBuilder.literal(centreId))
            .build();
    }
}
