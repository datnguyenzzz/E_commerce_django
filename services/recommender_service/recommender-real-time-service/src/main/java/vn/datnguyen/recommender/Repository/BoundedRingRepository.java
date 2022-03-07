package vn.datnguyen.recommender.Repository;

import java.util.UUID;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;

public class BoundedRingRepository implements BoundedRingInterface {

    public SimpleStatement createRowIfNotExists() {
        return SchemaBuilder.createTable(BOUNDED_RING_ROW)
            .ifNotExists()
            .withPartitionKey(CENTRE_ID, DataTypes.INT)
            .withPartitionKey(RING_ID, DataTypes.UUID)
            .withClusteringColumn(UPPER_BOUND_RANGE, DataTypes.DOUBLE)
            .withColumn(LOWER_BOUND_RANGE, DataTypes.DOUBLE)
            .withColumn(CAPACITY, DataTypes.INT)
            .build();
    }

    public SimpleStatement addNewBoundedRing(UUID ringId, int centreId, double lbRange, double ubRange, int capacity) {
        return QueryBuilder.insertInto(BOUNDED_RING_ROW)
            .value(RING_ID, QueryBuilder.literal(ringId))
            .value(CENTRE_ID, QueryBuilder.literal(centreId))
            .value(LOWER_BOUND_RANGE, QueryBuilder.literal(lbRange))
            .value(UPPER_BOUND_RANGE, QueryBuilder.literal(ubRange))
            .value(CAPACITY, QueryBuilder.literal(capacity))
            .build();
    }

    public SimpleStatement findBoundedRingById(UUID ringId, int centreId) {
        return QueryBuilder.selectFrom(BOUNDED_RING_ROW).all()
            .where(
                Relation.column(RING_ID).isEqualTo(QueryBuilder.literal(ringId)),
                Relation.column(CENTRE_ID).isEqualTo(QueryBuilder.literal(centreId))
            )
            .build().setConsistencyLevel(ConsistencyLevel.QUORUM);
    }

    public SimpleStatement findAllBoundedRingInCentre(int centreId) {
        return QueryBuilder.selectFrom(BOUNDED_RING_ROW).all()
            .where(
                Relation.column(CENTRE_ID).isEqualTo(QueryBuilder.literal(centreId))
            )
            .allowFiltering()
            .build().setConsistencyLevel(ConsistencyLevel.QUORUM);
    }

    public SimpleStatement findBoundedRingByIdAndRange(int centreId, double ubRange) {
        return QueryBuilder.selectFrom(BOUNDED_RING_ROW).all()
        .where(
            Relation.column(CENTRE_ID).isEqualTo(QueryBuilder.literal(centreId)),
            Relation.column(UPPER_BOUND_RANGE).isEqualTo(QueryBuilder.literal(ubRange))
        )
        .allowFiltering()
        .build().setConsistencyLevel(ConsistencyLevel.QUORUM);
    }
    /*
    public SimpleStatement updateBoundedRingCapacityById(UUID ringId, int centreId, double ubRange, int capacity) {
        return QueryBuilder.update(BOUNDED_RING_ROW)
            .set(
                Assignment.setColumn(CAPACITY, QueryBuilder.literal(capacity))
            )
            .where(
                Relation.column(RING_ID).isEqualTo(QueryBuilder.literal(ringId)),
                Relation.column(CENTRE_ID).isEqualTo(QueryBuilder.literal(centreId)),
                Relation.column(UPPER_BOUND_RANGE).isEqualTo(QueryBuilder.literal(ubRange))
            )
            .build().setConsistencyLevel(ConsistencyLevel.QUORUM);
    }*/

    public SimpleStatement deleteBoundedRingById(UUID ringId, int centreId) {
        return QueryBuilder.deleteFrom(BOUNDED_RING_ROW)
            .where(
                Relation.column(RING_ID).isEqualTo(QueryBuilder.literal(ringId)),
                Relation.column(CENTRE_ID).isEqualTo(QueryBuilder.literal(centreId))
            )
            .build();
    }
}
