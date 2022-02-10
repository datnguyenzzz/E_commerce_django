package vn.datnguyen.recommender.Repository;


import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
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
        return new ClientRatingRepository(session);
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
}
