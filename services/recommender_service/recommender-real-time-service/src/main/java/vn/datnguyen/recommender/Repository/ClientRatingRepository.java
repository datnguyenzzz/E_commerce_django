package vn.datnguyen.recommender.Repository;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;

public class ClientRatingRepository implements ClientRatingInterface {
    
    private static final String CLIENT_RATING_ROW = "client_rating_row";
    private static final String CLIENT_ID = "client_id";
    private static final String ITEM_ID = "item_id";
    private static final String RATING = "rating";

    private CqlSession session; 

    public ClientRatingRepository(CqlSession session) {
        this.session = session;
    }

    public CqlSession getSession() {
        return session;
    }

    @Override
    public SimpleStatement createRowIfExists() {
        return SchemaBuilder.createTable(CLIENT_RATING_ROW)
            .ifNotExists()
            .withPartitionKey(CLIENT_ID, DataTypes.TEXT)
            .withColumn(ITEM_ID, DataTypes.TEXT)
            .withColumn(RATING, DataTypes.INT)
            .build();
    }

    @Override
    public SimpleStatement createIndexOnItemId() {
        return SchemaBuilder.createIndex()
            .ifNotExists()
            .onTable(CLIENT_RATING_ROW)
            .andColumn(ITEM_ID)
            .build();
    }
}
