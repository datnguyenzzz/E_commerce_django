package vn.datnguyen.recommender.Repository;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;

import vn.datnguyen.recommender.Models.ItemCount;

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

    @Override
    public SimpleStatement findByItemId(String itemId) {
        return QueryBuilder.selectFrom(ITEM_COUNT_ROW).all()
            .where(
                Relation.column(ITEM_ID).isEqualTo(QueryBuilder.literal(itemId))
            )
            .build().setConsistencyLevel(ConsistencyLevel.ONE);
    }

    @Override
    public SimpleStatement insertNewScore(ItemCount itemCount) {
        return QueryBuilder.insertInto(ITEM_COUNT_ROW)
            .value(ITEM_ID, QueryBuilder.literal(itemCount.getItemId()))
            .value(SCORE, QueryBuilder.literal(itemCount.getScore()))
            .build();

    }

    @Override
    public SimpleStatement updateIncrScore(String itemId, int deltaScore) {
        return QueryBuilder.update(ITEM_COUNT_ROW)
            .increment(SCORE, QueryBuilder.literal(deltaScore))
            .where(
                Relation.column(ITEM_ID).isEqualTo(QueryBuilder.literal(itemId))
            )
            .build().setConsistencyLevel(ConsistencyLevel.ONE);
    }

    @Override
    public ItemCount convertToPojo(Row row) {
        return new ItemCount(
            row.getString(ITEM_ID),
            row.getInt(SCORE)
        );
    }
}
