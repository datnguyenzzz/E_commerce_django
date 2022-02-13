package vn.datnguyen.recommender.Bolt;

import java.util.Map;

import com.datastax.oss.driver.api.core.CqlSession;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import vn.datnguyen.recommender.CassandraConnector;
import vn.datnguyen.recommender.Models.Event;
import vn.datnguyen.recommender.Repository.KeyspaceRepository;
import vn.datnguyen.recommender.Repository.RepositoryFactory;
import vn.datnguyen.recommender.utils.CustomProperties;

/**
 * coratingUPQ = min(Rup, Ruq)

    exist P . then update all (P-Q) and (Q-P). 
    R_new_up < Ruq -> += delta(R) 

    ---------------------------
    not exist P 
    co-rating(P,P) = R_new_up

    get all Q . then insert all (P-Q) and (Q-P)
    R_new_up < Ruq -> R_new_up
 */

public class CoRatingBolt extends BaseRichBolt {
    
    private final Logger logger = LoggerFactory.getLogger(CoRatingBolt.class);
    private final static CustomProperties customProperties = CustomProperties.getInstance();
    private OutputCollector collector;
    //VALUE FIELDS
    private final static String OLD_RATING = customProperties.getProp("OLD_RATING");
    private final static String EVENT_FIELD = customProperties.getProp("EVENT_FIELD");
    private final static String KEYSPACE_FIELD = customProperties.getProp("KEYSPACE_FIELD");
    private final static String NUM_NODE_REPLICAS_FIELD = customProperties.getProp("NUM_NODE_REPLICAS_FIELD");
    //CASSANDRA PROPS
    private final static String CASS_NODE = customProperties.getProp("CASS_NODE");
    private final static String CASS_PORT = customProperties.getProp("CASS_PORT");
    private final static String CASS_DATA_CENTER = customProperties.getProp("CASS_DATA_CENTER");

    private RepositoryFactory repositoryFactory;

    private void launchCassandraKeyspace() {
        CassandraConnector connector = new CassandraConnector();
        connector.connect(CASS_NODE, Integer.parseInt(CASS_PORT), CASS_DATA_CENTER);
        CqlSession session = connector.getSession();

        this.repositoryFactory = new RepositoryFactory(session);
        KeyspaceRepository keyspaceRepository = this.repositoryFactory.getKeyspaceRepository();
        keyspaceRepository.createAndUseKeyspace(KEYSPACE_FIELD, Integer.parseInt(NUM_NODE_REPLICAS_FIELD));
        logger.info("CREATE AND USE KEYSPACE SUCCESSFULLY keyspace in **** CoRatingBolt ****");
    }
    
    @Override
    public void prepare(Map<String, Object> map, TopologyContext TopologyContext, OutputCollector collector) {
        this.collector = collector;

        launchCassandraKeyspace();
    }
    
    @Override
    public void execute(Tuple input) {
        Event incomeEvent = (Event) input.getValueByField(EVENT_FIELD);
        int oldRating = (int) input.getValueByField(OLD_RATING);
        int newRating = incomeEvent.getWeight();
        int deltaRating = newRating - oldRating;
        String itemId = incomeEvent.getClientId();

        logger.info("********* CoRatingBolt **********" + incomeEvent + " with old rating = " + oldRating);
        collector.ack(input);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sink-bolt"));
    }
}
