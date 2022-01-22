package vn.datnguyen.recommender.Service;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import static java.sql.DriverManager.getConnection;

@Service
public class WALPollingService {

    private final Logger logger = LoggerFactory.getLogger(WALPollingService.class);

    @Value("${spring.datasource.username}")
    private String user;

    @Value("${spring.datasource.password}")
    private String password;

    @Value("${spring.datasource.url}")
    private String url;

    public WALPollingService() {}

    public void streamPhysicalWAL() throws SQLException {

        Properties props = new Properties();
        PGProperty.USER.set(props, user);
        PGProperty.PASSWORD.set(props, password);
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "9.4");
        PGProperty.REPLICATION.set(props, "database");
        PGProperty.PREFER_QUERY_MODE.set(props, "simple");

        try (Connection dbConnection = getConnection(url, props)) {
            PGConnection replicaConnection = dbConnection.unwrap(PGConnection.class);

            //create replication slot 
            replicaConnection.getReplicationAPI()
                .createReplicationSlot()
                .logical()
                .withSlotName("wal_miner_logical_slot")
                .withOutputPlugin("test_decoding")
                .make();
                
            while (true) {
                logger.info("TRANSACTION-LOG-TAILING: Start polling from WAL");
            }
        }
    }
}
