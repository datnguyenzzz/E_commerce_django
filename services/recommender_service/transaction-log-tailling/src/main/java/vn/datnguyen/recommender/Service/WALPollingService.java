package vn.datnguyen.recommender.Service;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.PGReplicationStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import static java.sql.DriverManager.getConnection;

import java.nio.ByteBuffer;

@Service
public class WALPollingService {

    private final Logger logger = LoggerFactory.getLogger(WALPollingService.class);

    @Value("${spring.datasource.username}")
    private String user;

    @Value("${spring.datasource.password}")
    private String password;

    @Value("${spring.datasource.url}")
    private String url;

    private final String logicalSlotname = "wal_miner_logical_slot";
    private final int feedbackInterval = 20;

    public WALPollingService() {}

    public void streamPhysicalWAL() throws SQLException, InterruptedException {
        logger.info("TRANSACTION-LOG-TAILING: Start polling from WAL");

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
                .withSlotName(logicalSlotname)
                .withOutputPlugin("test_decoding")
                .make();

            //create replication stream 

            PGReplicationStream stream = replicaConnection.getReplicationAPI()
                .replicationStream()
                .logical()
                .withSlotName(logicalSlotname)
                .withSlotOption("include-xids", false)
                .withSlotOption("skip-empty-xacts", true)
                .withStatusInterval(feedbackInterval, TimeUnit.SECONDS)
                .start();
            
            //non blocking receive message
            while (true) {
                ByteBuffer msg = stream.readPending();

                if (msg == null) {
                    TimeUnit.MILLISECONDS.sleep(10L);
                    continue;
                }

                int offset = msg.arrayOffset();
                byte[] source = msg.array(); 
                int length = source.length - offset; 

                String receivedString = new String(source, offset, length);

                logger.info("WAL-MINER: Received string= " + receivedString);

                //feedback to db
                stream.setAppliedLSN(stream.getLastReceiveLSN());
                stream.setFlushedLSN(stream.getLastReceiveLSN());
            }
        }
    }
}
