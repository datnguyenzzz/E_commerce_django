package vn.datnguyen.recommender.Repository;

import java.util.List;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;

public class IndexesCoordRepository implements IndexesCoordInterface {
    private final static String INDEXES_COORD_ROW = "indexes_coord_row";
    private final static String CENTRE_ID = "centre_id";
    private final static String CENTRE_COORD = "centre_coord";

    public SimpleStatement createRowIfNotExists() {
        return SchemaBuilder.createTable(INDEXES_COORD_ROW)
            .ifNotExists()
            .withPartitionKey(CENTRE_ID, DataTypes.INT)
            .withColumn(CENTRE_COORD, DataTypes.listOf(DataTypes.INT))
            .build();
    }

    public SimpleStatement insertNewIndex(int id, List<Integer> coord) {
        return QueryBuilder.insertInto(INDEXES_COORD_ROW)
            .value(CENTRE_ID, QueryBuilder.literal(id))
            .value(CENTRE_COORD, QueryBuilder.literal(coord))
            .build();
    }
}
