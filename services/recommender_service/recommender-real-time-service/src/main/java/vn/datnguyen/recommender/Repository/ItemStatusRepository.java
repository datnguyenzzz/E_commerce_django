package vn.datnguyen.recommender.Repository;

import java.util.List;
import java.util.UUID;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;

public class ItemStatusRepository implements ItemStatusInterface {

    public SimpleStatement createRowIfNotExists() {
        return SchemaBuilder.createTable(ITEM_STATUS_ROW)
            .ifNotExists()
            .withPartitionKey(BOUNDED_RING_ID, DataTypes.UUID)
            .withPartitionKey(CENTRE_ID, DataTypes.INT)
            .withClusteringColumn(ITEM_ID, DataTypes.TEXT)
            .withColumn(ADD_BY_CLIENT_ID, DataTypes.TEXT)
            .withColumn(DISTANCE_TO_CENTRE, DataTypes.DOUBLE)
            .withColumn(VECTOR_PROPERTIES, DataTypes.listOf(DataTypes.INT))
            .build();
    }

    public SimpleStatement addNewItemStatus(String itemId, String clientId, UUID boundedRingId, int centreId, double dist, List<Integer> properties) {
        return QueryBuilder.insertInto(ITEM_STATUS_ROW)
            .value(ITEM_ID, QueryBuilder.literal(itemId))
            .value(ADD_BY_CLIENT_ID, QueryBuilder.literal(clientId))
            .value(BOUNDED_RING_ID, QueryBuilder.literal(boundedRingId))
            .value(CENTRE_ID, QueryBuilder.literal(centreId))
            .value(DISTANCE_TO_CENTRE, QueryBuilder.literal(dist))
            .value(VECTOR_PROPERTIES, QueryBuilder.literal(properties))
            .build();
    }

    public SimpleStatement findAllByRingId(UUID boundedRingId, int centreId) {
        return QueryBuilder.selectFrom(ITEM_STATUS_ROW).all()
            .where(
                Relation.column(BOUNDED_RING_ID).isEqualTo(QueryBuilder.literal(boundedRingId)),
                Relation.column(CENTRE_ID).isEqualTo(QueryBuilder.literal(centreId))
            )
            .build().setConsistencyLevel(ConsistencyLevel.QUORUM);
    }

    public SimpleStatement deleteItemStatus(String itemId, UUID boundedRingId, int centreId) {
        return QueryBuilder.deleteFrom(ITEM_STATUS_ROW)
            .where(
                Relation.column(BOUNDED_RING_ID).isEqualTo(QueryBuilder.literal(boundedRingId)),
                Relation.column(CENTRE_ID).isEqualTo(QueryBuilder.literal(centreId)),
                Relation.column(ITEM_ID).isEqualTo(QueryBuilder.literal(itemId))
            )
            .build();
    }
}
