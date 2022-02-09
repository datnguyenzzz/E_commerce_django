package vn.datnguyen.recommender.Repository;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeyspaceRepository implements KeyspaceInterface {
    private Logger logger = LoggerFactory.getLogger(KeyspaceRepository.class);
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

        logger.info("CREATE AND USE KEYSPACE SUCCESSFULLY with keyspace " + name);
    }

}
