package vn.datnguyen.recommender.Repository;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.condition.Condition;
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


    public CoRatingRepository() {}

    @Override
    public SimpleStatement createRowIfNotExists() {
        return SchemaBuilder.createTable(CO_RATING_ROW)
            .ifNotExists()
            .withPartitionKey(ITEM_1_ID, DataTypes.TEXT)
            .withClusteringColumn(ITEM_2_ID, DataTypes.TEXT)
            .withClusteringColumn(CLIENT_ID, DataTypes.TEXT)
            .withColumn(SCORE, DataTypes.COUNTER)
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
        return QueryBuilder.selectFrom(CO_RATING_ROW).all()
            .where(
                Relation.column(ITEM_1_ID).isEqualTo(QueryBuilder.literal(item1Id)),
                Relation.column(CLIENT_ID).isEqualTo(QueryBuilder.literal(clientId))
            )
            .build();
    } 

    @Override
    public SimpleStatement updateItem1Score(String itemId, String clientId, int newRating, int deltaRating) {
        return QueryBuilder.update(CO_RATING_ROW)
            .set(
                Assignment.increment(SCORE, QueryBuilder.literal(deltaRating))
            )
            .where(
                Relation.column(ITEM_1_ID).isEqualTo(QueryBuilder.literal(itemId)),
                Relation.column(CLIENT_ID).isEqualTo(QueryBuilder.literal(clientId))
            )
            .if_(
                Condition.column(RATING_ITEM_2).isGreaterThan(QueryBuilder.literal(newRating))
            )
            .build();
    }

    @Override
    public SimpleStatement updateItem2Score(String itemId, String clientId, int newRating, int deltaRating) {
        return QueryBuilder.update(CO_RATING_ROW)
            .set(
                Assignment.increment(SCORE, QueryBuilder.literal(deltaRating))
            )
            .where(
                Relation.column(ITEM_2_ID).isEqualTo(QueryBuilder.literal(itemId)),
                Relation.column(CLIENT_ID).isEqualTo(QueryBuilder.literal(clientId))
            )
            .if_(
                Condition.column(RATING_ITEM_1).isGreaterThan(QueryBuilder.literal(newRating))
            )
            .build();
    }
}
