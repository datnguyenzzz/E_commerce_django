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
import vn.datnguyen.recommender.Repository.KeyspaceRepository;
import vn.datnguyen.recommender.Repository.RepositoryFactory;
import vn.datnguyen.recommender.utils.CustomProperties;

public class SimilaritiesBolt extends BaseRichBolt {
    
    private final Logger logger = LoggerFactory.getLogger(SimilaritiesBolt.class);
    private final static CustomProperties customProperties = CustomProperties.getInstance();
    private static final String ITEM_COUNT_BOLT = customProperties.getProp("ITEM_COUNT_BOLT");
    private static final String PAIR_COUNT_BOLT = customProperties.getProp("PAIR_COUNT_BOLT");
    //Item count
    private final static String ITEM_ID_FIELD = customProperties.getProp("ITEM_ID_FIELD");
    private final static String OLD_ITEM_COUNT = customProperties.getProp("OLD_ITEM_COUNT");
    private final static String NEW_ITEM_COUNT = customProperties.getProp("NEW_ITEM_COUNT");
    //pair count 
    private final static String ITEM_1_ID_FIELD = customProperties.getProp("ITEM_1_ID_FIELD");
    private final static String ITEM_2_ID_FIELD = customProperties.getProp("ITEM_2_ID_FIELD");
    private final static String OLD_PAIR_COUNT = customProperties.getProp("OLD_PAIR_COUNT");
    private final static String NEW_PAIR_COUNT = customProperties.getProp("NEW_PAIR_COUNT");
    //CASSANDRA PROPS
    private final static String CASS_NODE = customProperties.getProp("CASS_NODE");
    private final static String CASS_PORT = customProperties.getProp("CASS_PORT");
    private final static String CASS_DATA_CENTER = customProperties.getProp("CASS_DATA_CENTER");
    //VALUE FIELDS
    private final static String KEYSPACE_FIELD = customProperties.getProp("KEYSPACE_FIELD");
    private final static String NUM_NODE_REPLICAS_FIELD = customProperties.getProp("NUM_NODE_REPLICAS_FIELD");
    //
    private OutputCollector collector;
    private RepositoryFactory repositoryFactory;
    
    @Override
    public void prepare(Map<String, Object> map, TopologyContext TopologyContext, OutputCollector collector) {
        this.collector = collector;

        launchCassandraKeyspace();

        createTableIfNotExists();
    }

    private void launchCassandraKeyspace() {
        CassandraConnector connector = new CassandraConnector();
        connector.connect(CASS_NODE, Integer.parseInt(CASS_PORT), CASS_DATA_CENTER);
        CqlSession session = connector.getSession();

        this.repositoryFactory = new RepositoryFactory(session);
        KeyspaceRepository keyspaceRepository = this.repositoryFactory.getKeyspaceRepository();
        keyspaceRepository.createAndUseKeyspace(KEYSPACE_FIELD, Integer.parseInt(NUM_NODE_REPLICAS_FIELD));
        logger.info("CREATE AND USE KEYSPACE SUCCESSFULLY keyspace in **** ItemCountBolt ****");
    }

    private void createTableIfNotExists() {}
    
    @Override
    public void execute(Tuple input) {
        String inputSource = input.getSourceComponent();
        logger.info("************ SimilaritiesBolt *************: Tuple input from " + inputSource);

        if (inputSource.equals(ITEM_COUNT_BOLT)) {
            String itemId = (String) input.getValueByField(ITEM_ID_FIELD);
            int oldItemCount = (int) input.getValueByField(OLD_ITEM_COUNT);
            int newItemCount = (int) input.getValueByField(NEW_ITEM_COUNT);
            logger.info("************ SimilaritiesBolt *************: Values - " + itemId + " . " + oldItemCount + " . " + newItemCount);
        } else if (inputSource.equals(PAIR_COUNT_BOLT)) {
            String item1Id = (String) input.getValueByField(ITEM_1_ID_FIELD);
            String item2Id = (String) input.getValueByField(ITEM_2_ID_FIELD);
            int oldPairCount = (int) input.getValueByField(OLD_PAIR_COUNT);
            int newPairCount = (int) input.getValueByField(NEW_PAIR_COUNT);
            logger.info("************ SimilaritiesBolt *************: Values - " + item1Id + " . " + item2Id + " . " + oldPairCount + " . " + newPairCount);
        }
        collector.ack(input);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sink-bolt"));
    }
}
