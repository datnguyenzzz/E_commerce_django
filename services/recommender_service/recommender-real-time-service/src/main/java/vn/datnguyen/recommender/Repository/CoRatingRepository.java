package vn.datnguyen.recommender.Repository;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;

public class CoRatingRepository implements CoRatingInterface {

    private static final String CO_RATING_ROW = "co_rating_row";
    private static final String CLIENT_ID = "client_id";
    private static final String ITEM_1_ID = "item_1_id";
    private static final String ITEM_2_ID = "item_2_id";
    private static final String SCORE = "score";


    public CoRatingRepository() {}

    @Override
    public SimpleStatement createRowIfNotExists() {
        return SchemaBuilder.createTable(CO_RATING_ROW)
            .ifNotExists()
            .withPartitionKey(ITEM_1_ID, DataTypes.TEXT)
            .withPartitionKey(ITEM_2_ID, DataTypes.TEXT)
            .withColumn(CLIENT_ID, DataTypes.TEXT)
            .withColumn(SCORE, DataTypes.INT)
            .build();
    }
}
