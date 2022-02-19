package vn.datnguyen.recommender.Repository;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.update.Assignment;

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
            .withClusteringColumn(ITEM_2_ID, DataTypes.TEXT)
            .withColumn(SCORE, DataTypes.DOUBLE)
            .build();
    }

    @Override
    public SimpleStatement findBy(String item1Id, String item2Id) {
        return QueryBuilder.selectFrom(SIMILARITIES_ROW).column(SCORE)
            .where(
                Relation.column(ITEM_1_ID).isEqualTo(QueryBuilder.literal(item1Id)),
                Relation.column(ITEM_2_ID).isEqualTo(QueryBuilder.literal(item2Id))
            )
            .build().setConsistencyLevel(ConsistencyLevel.QUORUM);
    }

    @Override
    public SimpleStatement initScore(String item1Id, String item2Id, double score) {
        return QueryBuilder.insertInto(SIMILARITIES_ROW)
            .value(ITEM_1_ID, QueryBuilder.literal(item1Id))
            .value(ITEM_2_ID, QueryBuilder.literal(item2Id))
            .value(SCORE, QueryBuilder.literal(score))
            .build();
    }

    @Override
    public SimpleStatement updateScore(String item1Id, String item2Id, double newScore) {
        return QueryBuilder.update(SIMILARITIES_ROW)
            .set(
                Assignment.setColumn(SCORE, QueryBuilder.literal(newScore))
            )
            .where(
                Relation.column(ITEM_1_ID).isEqualTo(QueryBuilder.literal(item1Id)),
                Relation.column(ITEM_2_ID).isEqualTo(QueryBuilder.literal(item2Id))
            )
            .build().setConsistencyLevel(ConsistencyLevel.QUORUM); 
    }

    @Override
    public SimpleStatement findAllItemId() {
        return QueryBuilder.selectFrom(SIMILARITIES_ROW).column(ITEM_1_ID)
            .groupBy(ITEM_1_ID)
            .build();
    }

    @Override
    public SimpleStatement findByItem1Id(String item1Id) {
        return QueryBuilder.selectFrom(SIMILARITIES_ROW).columns(ITEM_2_ID, SCORE)
            .where(
                Relation.column(ITEM_1_ID).isEqualTo(QueryBuilder.literal(item1Id))
            )
            .build();
    }

    @Override
    public SimpleStatement findByItem2Id(String item2Id) {
        return QueryBuilder.selectFrom(SIMILARITIES_ROW).columns(ITEM_1_ID, SCORE)
            .where(
                Relation.column(ITEM_2_ID).isEqualTo(QueryBuilder.literal(item2Id))
            )
            .allowFiltering()
            .build();
    }
}
