package vn.datnguyen.recommender.Repository;

import java.util.concurrent.CompletionStage;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeyspaceRepository implements KeyspaceInterface {
    private Logger logger = LoggerFactory.getLogger(KeyspaceRepository.class);
    private CompletionStage<CqlSession> sessionStage;
    private CqlSession session;

    public KeyspaceRepository(CqlSession session, CompletionStage<CqlSession> sessionStage) {
        this.sessionStage = sessionStage;
        this.session = session;
    }

    @Override
    public CompletionStage<AsyncResultSet> createAndUseKeyspace(String name, int numReplicas) {
        CompletionStage<AsyncResultSet> createKeyspaceStage = 
            sessionStage.thenComposeAsync(
                sess -> sess.executeAsync(
                    SchemaBuilder.createKeyspace(name)
                        .ifNotExists()
                        .withSimpleStrategy(numReplicas)
                        .build()
                )
        );

        logger.info("********** CREATE KEYSPACE STAGE = " + createKeyspaceStage);

        CompletionStage<AsyncResultSet> useKeyspaceStage = 
            createKeyspaceStage.thenComposeAsync(
                f -> {
                    return session.executeAsync("USE" + CqlIdentifier.fromCql(name));
                }
            );
        
        logger.info("********** USE KEYSPACE STAGE = " + useKeyspaceStage);
        return useKeyspaceStage;
    }
}
