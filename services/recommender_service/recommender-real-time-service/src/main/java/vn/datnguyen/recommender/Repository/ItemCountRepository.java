package vn.datnguyen.recommender.Repository;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;

public class ItemCountRepository implements ItemCountInterface {

    private static final String ITEM_COUNT_ROW = "item_count_row";
    private static final String ITEM_ID = "item_id";
    private static final String SCORE = "score";

    private CqlSession session; 

    public ItemCountRepository(CqlSession session) {
        this.session = session;
    }

    public CqlSession getSession() {
        return session;
    }

    @Override
    public SimpleStatement createRowIfNotExists() {
        return SchemaBuilder.createTable(ITEM_COUNT_ROW)
            .ifNotExists()
            .withPartitionKey(ITEM_ID, DataTypes.TEXT)
            .withColumn(SCORE, DataTypes.INT)
            .build();
    }
}
