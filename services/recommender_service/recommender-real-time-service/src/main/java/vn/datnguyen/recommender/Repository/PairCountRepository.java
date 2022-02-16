package vn.datnguyen.recommender.Repository;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.update.Assignment;

public class PairCountRepository implements PairCountInterface {

    //pair count row
    private final static String PAIR_COUNT_ROW = "pair_count_row";
    private final static String ITEM_1_ID = "item_1_id";
    private final static String ITEM_2_ID = "item_2_id";
    private final static String SCORE = "score";

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

    @Override
    public SimpleStatement updateScore(String item1Id, String item2Id, int deltaScore) {
        return QueryBuilder.update(PAIR_COUNT_ROW)
            .set(
                Assignment.increment(SCORE, QueryBuilder.literal(deltaScore))
            )
            .where(
                Relation.column(ITEM_1_ID).isEqualTo(QueryBuilder.literal(item1Id)),
                Relation.column(ITEM_2_ID).isEqualTo(QueryBuilder.literal(item2Id))
            )
            .build();
    }

    @Override
    public SimpleStatement getCurrentScore(String item1Id, String item2Id) {
        return QueryBuilder.selectFrom(PAIR_COUNT_ROW).column(SCORE)
            .where(
                Relation.column(ITEM_1_ID).isEqualTo(QueryBuilder.literal(item1Id)),
                Relation.column(ITEM_2_ID).isEqualTo(QueryBuilder.literal(item2Id))
            )
            .build();
    }
}

