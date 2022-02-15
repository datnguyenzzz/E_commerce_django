package vn.datnguyen.recommender.Bolt;

import java.util.List;
import java.util.Map;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import vn.datnguyen.recommender.CassandraConnector;
import vn.datnguyen.recommender.Models.Event;
import vn.datnguyen.recommender.Repository.KeyspaceRepository;
import vn.datnguyen.recommender.Repository.PairCountRepository;
import vn.datnguyen.recommender.Repository.RepositoryFactory;
import vn.datnguyen.recommender.utils.CustomProperties;

/**
 * PairCount[p,q] = sum(CoRating[u,p,q])
 * => += delta_CoRating[u,p,q]
 */
public class PairCountBolt extends BaseRichBolt {
    
    private final Logger logger = LoggerFactory.getLogger(PairCountBolt.class);
    private final static CustomProperties customProperties = CustomProperties.getInstance();
    //VALUE FIELDS
    private final static String EVENT_FIELD = customProperties.getProp("EVENT_FIELD");
    private final static String KEYSPACE_FIELD = customProperties.getProp("KEYSPACE_FIELD");
    private final static String NUM_NODE_REPLICAS_FIELD = customProperties.getProp("NUM_NODE_REPLICAS_FIELD");
    //CASSANDRA PROPS
    private final static String CASS_NODE = customProperties.getProp("CASS_NODE");
    private final static String CASS_PORT = customProperties.getProp("CASS_PORT");
    private final static String CASS_DATA_CENTER = customProperties.getProp("CASS_DATA_CENTER");
    //Table id 
    private final static String ITEM_2_ID = "item_2_id";
    private static final String DELTA_SCORE = "delta_score";

    private OutputCollector collector;
    private RepositoryFactory repositoryFactory;
    private PairCountRepository pairCountRepository;

    private void launchCassandraKeyspace() {
        CassandraConnector connector = new CassandraConnector();
        connector.connect(CASS_NODE, Integer.parseInt(CASS_PORT), CASS_DATA_CENTER);
        CqlSession session = connector.getSession();

        this.repositoryFactory = new RepositoryFactory(session);
        KeyspaceRepository keyspaceRepository = this.repositoryFactory.getKeyspaceRepository();
        keyspaceRepository.createAndUseKeyspace(KEYSPACE_FIELD, Integer.parseInt(NUM_NODE_REPLICAS_FIELD));
        logger.info("CREATE AND USE KEYSPACE SUCCESSFULLY keyspace in **** ItemCountBolt ****");
    }

    private void createTableIfNotExists() {
        SimpleStatement createTableStatement = this.pairCountRepository.createRowIfNotExists();
        this.repositoryFactory.executeStatement(createTableStatement, KEYSPACE_FIELD);
        logger.info("********* PairCountBolt **********: created table");
    }
    
    @Override
    public void prepare(Map<String, Object> map, TopologyContext TopologyContext, OutputCollector collector) {
        this.collector = collector;

        launchCassandraKeyspace();
        this.pairCountRepository = this.repositoryFactory.getPairCountRepository();
        createTableIfNotExists();
    }
    
    @Override
    public void execute(Tuple input) {
        Event incomeEvent = (Event) input.getValueByField(EVENT_FIELD);
        String clientId = incomeEvent.getClientId();
        String itemId = incomeEvent.getItemId();

        SimpleStatement findDeltaCoRatingStatement = this.pairCountRepository.findDeltaCoRating(clientId, itemId);
        ResultSet findDeltaCoRatingResult = this.repositoryFactory.executeStatement(findDeltaCoRatingStatement, KEYSPACE_FIELD);
        List<Row> findDeltaCoRating = findDeltaCoRatingResult.all();
        int rowFound = findDeltaCoRating.size();

        if (rowFound == 0) {
            logger.warn("********* PairCountBolt **********: CAN NOT FIND ANY CO-RATING CORRECTSPOND TO itemId = " + itemId);
        } else {

            BatchStatementBuilder batchUpdateScore = BatchStatement.builder(BatchType.LOGGED);

            for (Row r: findDeltaCoRating) {
                String item2Id = (String) this.repositoryFactory.getFromRow(r, ITEM_2_ID);
                int deltaScore = (int) this.repositoryFactory.getFromRow(r, DELTA_SCORE);

                SimpleStatement updateScore; 

                updateScore = this.pairCountRepository.updateScore(itemId, item2Id, deltaScore);
                batchUpdateScore.addStatement(updateScore);

                updateScore = this.pairCountRepository.updateScore(item2Id, itemId, deltaScore);
                batchUpdateScore.addStatement(updateScore);
            }
            BatchStatement allBatch = batchUpdateScore.build();
            logger.info("********* PairCountBolt **********: Attempt to execute " + allBatch.size() + " queries in batch");
            this.repositoryFactory.executeStatement(allBatch, KEYSPACE_FIELD);
        }

        logger.info("********* PairCountBolt **********" + incomeEvent);
        
        Values values = new Values(incomeEvent);
        collector.emit(values);
        collector.ack(input);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sink-bolt"));
    }
}
