package vn.datnguyen.recommender.Repository;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;

public class PairCountRepository implements PairCountInterface {

    //pair count row
    private final static String PAIR_COUNT_ROW = "pair_count_row";
    private final static String ITEM_1_ID = "item_1_id";
    private final static String ITEM_2_ID = "item_2_id";
    private final static String SCORE = "score";
    //co rating row
    private static final String CO_RATING_ROW = "co_rating_row";
    private static final String DELTA_SCORE = "delta_score";
    private static final String CLIENT_ID = "client_id";

    public PairCountRepository () {}

    @Override
    public SimpleStatement createRowIfNotExists() {
        return SchemaBuilder.createTable(PAIR_COUNT_ROW)
            .ifNotExists()
            .withPartitionKey(ITEM_1_ID, DataTypes.TEXT)
            .withPartitionKey(ITEM_2_ID, DataTypes.TEXT)
            .withColumn(SCORE, DataTypes.COUNTER)
            .build();
    }
}
