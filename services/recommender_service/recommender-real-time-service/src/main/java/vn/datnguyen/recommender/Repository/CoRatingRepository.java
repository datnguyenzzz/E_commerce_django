package vn.datnguyen.recommender.Repository;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;

public class CoRatingRepository implements CoRatingInterface {

    private static final String CO_RATING_ROW = "co_rating_row";
    private static final String INDEX_ROW = "index_co_rating";
    private static final String CLIENT_ID = "client_id";
    private static final String ITEM_1_ID = "item_1_id";
    private static final String ITEM_2_ID = "item_2_id";
    private static final String RATING_ITEM_1 = "rating_item_1";
    private static final String RATING_ITEM_2 = "rating_item_2";
    private static final String SCORE = "score";


    public CoRatingRepository() {}

    @Override
    public SimpleStatement createRowIfNotExists() {
        return SchemaBuilder.createTable(CO_RATING_ROW)
            .ifNotExists()
            .withPartitionKey(ITEM_1_ID, DataTypes.TEXT)
            .withClusteringColumn(ITEM_2_ID, DataTypes.TEXT)
            .withColumn(CLIENT_ID, DataTypes.TEXT)
            .withColumn(SCORE, DataTypes.INT)
            .withColumn(RATING_ITEM_1, DataTypes.INT)
            .withColumn(RATING_ITEM_2, DataTypes.INT)
            .build();
    }

    @Override
    public SimpleStatement createIndexes() {
        return SchemaBuilder.createIndex(INDEX_ROW)
            .ifNotExists()
            .onTable(CO_RATING_ROW)
            .andColumn(ITEM_2_ID)
            .build();
    }
}
