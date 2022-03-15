package vn.datnguyen.recommender.Repository;

import java.util.ArrayList;
import java.util.List;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.update.Assignment;

public class IndexesCoordRepository implements IndexesCoordInterface {

    public SimpleStatement createRowIfNotExists() {
        return SchemaBuilder.createTable(INDEXES_COORD_ROW)
            .ifNotExists()
            .withPartitionKey(CENTRE_ID, DataTypes.INT)
            .withColumn(CENTRE_COORD, DataTypes.listOf(DataTypes.INT))
            .withColumn(CENTRE_UPPER_BOUND_RANGE_LIST, DataTypes.listOf(DataTypes.BIGINT))
            .build();
    }

    public SimpleStatement insertNewIndex(int id, List<Integer> coord) {

        List<Long> initial = new ArrayList<>();

        return QueryBuilder.insertInto(INDEXES_COORD_ROW)
            .value(CENTRE_ID, QueryBuilder.literal(id))
            .value(CENTRE_COORD, QueryBuilder.literal(coord))
            .value(CENTRE_UPPER_BOUND_RANGE_LIST, QueryBuilder.literal(initial))
            .build();
    }

    public SimpleStatement selectAllCentre() {
        return QueryBuilder.selectFrom(INDEXES_COORD_ROW).all()
            .build();
    }

    public SimpleStatement selectCentreById(int id) {
        return QueryBuilder.selectFrom(INDEXES_COORD_ROW).all()
            .where(
                Relation.column(CENTRE_ID).isEqualTo(QueryBuilder.literal(id))
            )
            .build();
    }

    public SimpleStatement updateUBRangeListById(int id, List<Long> ubRangeList) {
        return QueryBuilder.update(INDEXES_COORD_ROW)
            .set(
                Assignment.setColumn(CENTRE_UPPER_BOUND_RANGE_LIST, QueryBuilder.literal(ubRangeList))
            )
            .where(
                Relation.column(CENTRE_ID).isEqualTo(QueryBuilder.literal(id))
            )
            .build().setConsistencyLevel(ConsistencyLevel.QUORUM);
    }
}
