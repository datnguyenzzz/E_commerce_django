package vn.datnguyen.recommender.Repository;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.update.Assignment;

public class CoRatingRepository implements CoRatingInterface {

    private static final String CO_RATING_ROW = "co_rating_row";
    private static final String INDEX_ITEM2_ID = "index_co_rating_item_id";
    private static final String INDEX_CLIENT_ID = "index_co_rating_client_id";
    private static final String CLIENT_ID = "client_id";
    private static final String ITEM_1_ID = "item_1_id";
    private static final String ITEM_2_ID = "item_2_id";
    private static final String RATING_ITEM_1 = "rating_item_1";
    private static final String RATING_ITEM_2 = "rating_item_2";
    private static final String SCORE = "score";
    private static final String DELTA_SCORE = "delta_score";


    public CoRatingRepository() {}

    @Override
    public Object getFromRow(Row row, String col) {
        return row.getObject(col);
    }

    @Override
    public SimpleStatement createRowIfNotExists() {
        return SchemaBuilder.createTable(CO_RATING_ROW)
            .ifNotExists()
            .withPartitionKey(ITEM_1_ID, DataTypes.TEXT)
            .withClusteringColumn(ITEM_2_ID, DataTypes.TEXT)
            .withClusteringColumn(CLIENT_ID, DataTypes.TEXT)
            .withColumn(SCORE, DataTypes.INT)
            .withColumn(DELTA_SCORE, DataTypes.INT)
            .withColumn(RATING_ITEM_1, DataTypes.INT)
            .withColumn(RATING_ITEM_2, DataTypes.INT)
            .build();
    }

    @Override
    public SimpleStatement createIndexOnItemId() {
        return SchemaBuilder.createIndex(INDEX_ITEM2_ID)
            .ifNotExists()
            .onTable(CO_RATING_ROW)
            .andColumn(ITEM_2_ID)
            .build();
    }

    @Override
    public SimpleStatement createIndexOnClientId() {
        return SchemaBuilder.createIndex(INDEX_CLIENT_ID)
            .ifNotExists()
            .onTable(CO_RATING_ROW)
            .andColumn(CLIENT_ID)
            .build();
    }

    @Override
    public SimpleStatement findByItem1IdAndClientId(String item1Id, String clientId) {
        return QueryBuilder.selectFrom(CO_RATING_ROW).columns(ITEM_2_ID, RATING_ITEM_2)
            .where(
                Relation.column(ITEM_1_ID).isEqualTo(QueryBuilder.literal(item1Id)),
                Relation.column(CLIENT_ID).isEqualTo(QueryBuilder.literal(clientId))
            )
            .build();
    }

    @Override
    public SimpleStatement findByItem2IdAndClientId(String item2Id, String clientId) {
        return QueryBuilder.selectFrom(CO_RATING_ROW).columns(ITEM_1_ID, RATING_ITEM_1)
            .where(
                Relation.column(ITEM_2_ID).isEqualTo(QueryBuilder.literal(item2Id)),
                Relation.column(CLIENT_ID).isEqualTo(QueryBuilder.literal(clientId))
            )
            .allowFiltering()
            .build();
    }

    @Override
    public SimpleStatement findSetItemIdByClientId(String clientId) {
        return QueryBuilder.selectFrom(CO_RATING_ROW).columns(ITEM_1_ID, RATING_ITEM_1)
            .where(
                Relation.column(CLIENT_ID).isEqualTo(QueryBuilder.literal(clientId))
            )
            .groupBy(ITEM_1_ID)
            .build();
    }
    
    @Override
    public SimpleStatement updateItemScore(String item1Id, String item2Id, String clientId, int newScore, int deltaScore) {
        return QueryBuilder.update(CO_RATING_ROW)
            .set(
                Assignment.setColumn(SCORE, QueryBuilder.literal(newScore)),
                Assignment.setColumn(DELTA_SCORE, QueryBuilder.literal(deltaScore))
            )
            .where(
                Relation.column(ITEM_1_ID).isEqualTo(QueryBuilder.literal(item1Id)),
                Relation.column(ITEM_2_ID).isEqualTo(QueryBuilder.literal(item2Id)),
                Relation.column(CLIENT_ID).isEqualTo(QueryBuilder.literal(clientId))
            )
            .build();
    }

    @Override
    public SimpleStatement updateItem1Rating(String item1Id, String item2Id, String clientId, int newRating) {
        return QueryBuilder.update(CO_RATING_ROW)
            .set(
                Assignment.setColumn(RATING_ITEM_1, QueryBuilder.literal(newRating))
            )
            .where(
                Relation.column(ITEM_1_ID).isEqualTo(QueryBuilder.literal(item1Id)),
                Relation.column(ITEM_2_ID).isEqualTo(QueryBuilder.literal(item2Id)),
                Relation.column(CLIENT_ID).isEqualTo(QueryBuilder.literal(clientId))
            )
            .build();
    }
    @Override
    public SimpleStatement updateItem2Rating(String item1Id, String item2Id, String clientId, int newRating) {
        return QueryBuilder.update(CO_RATING_ROW)
            .set(
                Assignment.setColumn(RATING_ITEM_2, QueryBuilder.literal(newRating))
            )
            .where(
                Relation.column(ITEM_1_ID).isEqualTo(QueryBuilder.literal(item1Id)),
                Relation.column(ITEM_2_ID).isEqualTo(QueryBuilder.literal(item2Id)),
                Relation.column(CLIENT_ID).isEqualTo(QueryBuilder.literal(clientId))
            )
            .build();
    }

    @Override
    public SimpleStatement insertNewItemScore(String item1Id, String item2Id, String clientId) {
        return QueryBuilder.insertInto(CO_RATING_ROW)
            .value(ITEM_1_ID, QueryBuilder.literal(item1Id))
            .value(ITEM_2_ID, QueryBuilder.literal(item2Id))
            .value(CLIENT_ID, QueryBuilder.literal(clientId))
            .value(SCORE, QueryBuilder.literal(0))
            .value(RATING_ITEM_1, QueryBuilder.literal(0))
            .value(RATING_ITEM_2, QueryBuilder.literal(0))
            .value(DELTA_SCORE, QueryBuilder.literal(0))
            .build();
    }
}
