package vn.datnguyen.recommender.Repository;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;

public class SimilaritiesRepository implements SimilaritiesInterface {

    private final static String SIMILARITIES_ROW = "similarities_row";
    private final static String ITEM_1_ID = "item_1_id";
    private final static String ITEM_2_ID = "item_2_id";
    private final static String SCORE = "score";
    
    public SimilaritiesRepository () {}

    @Override
    public SimpleStatement createTableIfNotExists() {
        return SchemaBuilder.createTable(SIMILARITIES_ROW)
            .ifNotExists()
            .withPartitionKey(ITEM_1_ID, DataTypes.TEXT)
            .withPartitionKey(ITEM_2_ID, DataTypes.TEXT)
            .withClusteringColumn(SCORE, DataTypes.FLOAT)
            .withClusteringOrder(SCORE, ClusteringOrder.DESC)
            .build();
    }
}
