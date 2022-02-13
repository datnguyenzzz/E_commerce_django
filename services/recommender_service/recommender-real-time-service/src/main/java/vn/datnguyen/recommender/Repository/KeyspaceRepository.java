package vn.datnguyen.recommender.Repository;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;

public class KeyspaceRepository implements KeyspaceInterface {
    private CqlSession session;

    public KeyspaceRepository(CqlSession session) {
        this.session = session;
    }

    @Override
    public void createAndUseKeyspace(String name, int numReplicas) {
        session.execute(
            SchemaBuilder.createKeyspace(name)
                .ifNotExists()
                .withSimpleStrategy(numReplicas)
                .build());

        session.execute("USE " + CqlIdentifier.fromCql(name));
    }

}
