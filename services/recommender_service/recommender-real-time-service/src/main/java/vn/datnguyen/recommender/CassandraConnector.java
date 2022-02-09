package vn.datnguyen.recommender;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraConnector {

    private Logger logger = LoggerFactory.getLogger(CassandraConnector.class);

    private CompletionStage<CqlSession> sessionStage;

    private CqlSession session;

    public CassandraConnector () {}

    public void connect(String node, int port, String dataCenter) {
        CqlSessionBuilder builder = CqlSession.builder()
            .addContactPoint(new InetSocketAddress(node, port));

        sessionStage = builder.buildAsync();

        logger.info("Cassandra open !!!");
    }

    public CompletionStage<CqlSession> getSessionStage() {
        return sessionStage;
    }

    public CqlSession getSession() {
        return session;
    }

    public void close() throws Exception {
        sessionStage.thenAccept(CqlSession::close);
        ((CompletableFuture<CqlSession>) sessionStage).get();

        logger.info("Cassandra closed successfully");
    }
}
