package vn.datnguyen.recommender.Repository;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;

import vn.datnguyen.recommender.Models.ClientRating;

public class ClientRatingRepository implements ClientRatingInterface {
    
    private static final String CLIENT_RATING_ROW = "client_rating_row";
    private static final String INDEX_ROW = "index_client_rating";
    private static final String CLIENT_ID = "client_id";
    private static final String ITEM_ID = "item_id";
    private static final String RATING = "rating";

    public ClientRatingRepository() {}

    @Override
    public SimpleStatement createRowIfNotExists() {
        return SchemaBuilder.createTable(CLIENT_RATING_ROW)
            .ifNotExists()
            .withPartitionKey(CLIENT_ID, DataTypes.TEXT)
            .withClusteringColumn(ITEM_ID, DataTypes.TEXT)
            .withColumn(RATING, DataTypes.INT)
            .build();
    }

    @Override
    public SimpleStatement createIndexOnItemId() {
        return SchemaBuilder.createIndex(INDEX_ROW)
            .ifNotExists()
            .onTable(CLIENT_RATING_ROW)
            .andColumn(ITEM_ID)
            .build();
    }

    @Override
    public SimpleStatement findByClientIdAndItemId(String clientId, String itemId) {
        return QueryBuilder.selectFrom(CLIENT_RATING_ROW).all()
            .where(
                Relation.column(CLIENT_ID).isEqualTo(QueryBuilder.literal(clientId)),
                Relation.column(ITEM_ID).isEqualTo(QueryBuilder.literal(itemId))
            )
            .build().setConsistencyLevel(ConsistencyLevel.ONE);
    }

    @Override
    public SimpleStatement insertClientRating(ClientRating clientRating) {
        return QueryBuilder.insertInto(CLIENT_RATING_ROW)
            .value(CLIENT_ID, QueryBuilder.literal(clientRating.getClientId()))
            .value(ITEM_ID, QueryBuilder.literal(clientRating.getItemId()))
            .value(RATING, QueryBuilder.literal(clientRating.getRating()))
            .build();
    }

    @Override
    public SimpleStatement updateIfGreaterClientRating(ClientRating clientRating) {
        return QueryBuilder.update(CLIENT_RATING_ROW)
            .setColumn(RATING, QueryBuilder.literal(clientRating.getRating()))
            .where(
                Relation.column(CLIENT_ID).isEqualTo(QueryBuilder.literal(clientRating.getClientId())),
                Relation.column(ITEM_ID).isEqualTo(QueryBuilder.literal(clientRating.getItemId()))
            )
            .build().setConsistencyLevel(ConsistencyLevel.ONE);
    }

    @Override
    public ClientRating convertRowToPojo(Row row) {
        return new ClientRating(
            row.getString(CLIENT_ID),
            row.getString(ITEM_ID),
            row.getInt(RATING)
        );
    }
}
