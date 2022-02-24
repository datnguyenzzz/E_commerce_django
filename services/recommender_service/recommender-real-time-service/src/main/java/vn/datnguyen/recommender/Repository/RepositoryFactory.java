package vn.datnguyen.recommender.Repository;


import java.util.concurrent.CompletionStage;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

public class RepositoryFactory {

    private CqlSession session;

    public RepositoryFactory(CqlSession session) {
        this.session = session;
    }

    /**
     * 
     * @return Cassandra keyspace DAO
     */
    public KeyspaceRepository getKeyspaceRepository() {
        return new KeyspaceRepository(session);
    }

    /**
     * 
     * @return Cassandra ClienRating Row DAO
     */
    public ClientRatingRepository getClientRatingRepository() {
        return new ClientRatingRepository();
    }

    /**
     * 
     * @return Cassandra ItemCount DAO
     */
    public ItemCountRepository getItemCountRepository() {
        return new ItemCountRepository();
    }

    /**
     * 
     * @return Cassandra CoRating DAO
     */
    public CoRatingRepository getCoRatingRepository() {
        return new CoRatingRepository();
    } 

    /**
     * 
     * @return Cassandra PairCount DAO
     */
    public PairCountRepository getPairCountRepository() {
        return new PairCountRepository();
    }

    /**
     * 
     * @return Cassandra Similarities DAO
     */
    public SimilaritiesRepository getSimilaritiesRepository() {
        return new SimilaritiesRepository();
    }

    public IndexesCoordRepository getIndexesCoordRepository() {
        return new IndexesCoordRepository();
    }

    /**
     * 
     * @param statement
     * @param keyspaceName
     * @return execute cassandra statement within keyspace
     */
    public ResultSet executeStatement(SimpleStatement statement, String keyspaceName) {
        if (keyspaceName != null) {
            statement.setKeyspace(CqlIdentifier.fromCql(keyspaceName));
        }

        return this.session.execute(statement);
    }

    public ResultSet executeStatement(BatchStatement statement, String keyspaceName) {
        if (keyspaceName != null) {
            statement.setKeyspace(CqlIdentifier.fromCql(keyspaceName));
        }

        return this.session.execute(statement);
    }

    public CompletionStage<AsyncResultSet> asyncExecuteStatement(SimpleStatement statement, String keyspaceName) {
        if (keyspaceName != null) {
            statement.setKeyspace(CqlIdentifier.fromCql(keyspaceName));
        }

        return this.session.executeAsync(statement);
    }

    public CompletionStage<AsyncResultSet> asyncExecuteStatement(BatchStatement statement, String keyspaceName) {
        if (keyspaceName != null) {
            statement.setKeyspace(CqlIdentifier.fromCql(keyspaceName));
        }

        return this.session.executeAsync(statement);
    }

    /**
     * 
     * @param row Cassandra row
     * @param col key id
     * @return col value
     */
    public Object getFromRow(Row row, String col) {
        return row.getObject(col);
    }
}
