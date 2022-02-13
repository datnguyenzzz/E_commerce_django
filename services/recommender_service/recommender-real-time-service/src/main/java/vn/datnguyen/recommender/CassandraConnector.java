package vn.datnguyen.recommender;

import java.io.File;
import java.net.InetSocketAddress;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraConnector {

    private Logger logger = LoggerFactory.getLogger(CassandraConnector.class);
    private final static File cassandraConfig = new File("/cassandra.conf");

    private CqlSession session;

    public CassandraConnector () {}

    public void connect(String node, int port, String dataCenter) {
        CqlSessionBuilder builder = CqlSession.builder()
            .addContactPoint(new InetSocketAddress(node, port))
            .withLocalDatacenter(dataCenter);

        session = builder.withConfigLoader(DriverConfigLoader.fromFile(cassandraConfig))
            .build();

        logger.info("Cassandra open !!! on" + node + ":"+port+ " at " + dataCenter + " on session " + session);
    }

    public CqlSession getSession() {
        return session;
    }

    public void close() throws Exception {
        session.close();
        logger.info("Cassandra closed successfully");
    }
}
