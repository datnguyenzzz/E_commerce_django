package vn.datnguyen.recommender.Processors.ColaborativeFiltering;

import java.util.Map;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
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
    private final static String ITEM_1_ID_FIELD = customProperties.getProp("ITEM_1_ID_FIELD");
    private final static String ITEM_2_ID_FIELD = customProperties.getProp("ITEM_2_ID_FIELD");
    private final static String DELTA_SCORE_FIELD = customProperties.getProp("DELTA_SCORE_FIELD");
    private final static String OLD_PAIR_COUNT = customProperties.getProp("OLD_PAIR_COUNT");
    private final static String NEW_PAIR_COUNT = customProperties.getProp("NEW_PAIR_COUNT");
    private final static String KEYSPACE_FIELD = customProperties.getProp("KEYSPACE_FIELD");
    private final static String NUM_NODE_REPLICAS_FIELD = customProperties.getProp("NUM_NODE_REPLICAS_FIELD");
    //CASSANDRA PROPS
    private final static String CASS_NODE = customProperties.getProp("CASS_NODE");
    private final static String CASS_PORT = customProperties.getProp("CASS_PORT");
    private final static String CASS_DATA_CENTER = customProperties.getProp("CASS_DATA_CENTER");
    //Table id 
    private final static String SCORE = "score";

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

    @Override
    public void prepare(Map<String, Object> map, TopologyContext TopologyContext, OutputCollector collector) {
        this.collector = collector;

        launchCassandraKeyspace();
        this.pairCountRepository = this.repositoryFactory.getPairCountRepository();

        SimpleStatement createTableStatement = pairCountRepository.createRowIfNotExists();
        logger.info("********* PairCountBolt **********: created table");
        this.repositoryFactory.executeStatement(createTableStatement, KEYSPACE_FIELD);
    }

    private void initPairCountTable(String item1Id, String item2Id) {
        SimpleStatement createTableStatement = pairCountRepository.createRowIfNotExists();
        logger.info("********* PairCountBolt **********: created table");
        this.repositoryFactory.executeStatement(createTableStatement, KEYSPACE_FIELD);

        SimpleStatement findCurrentScore = this.pairCountRepository.getCurrentScore(item1Id, item2Id);
        ResultSet findCurrentScoreResult = this.repositoryFactory.executeStatement(findCurrentScore, KEYSPACE_FIELD);
        int rowFound = findCurrentScoreResult.getAvailableWithoutFetching();

        if (rowFound == 0) {
            SimpleStatement initCols = this.pairCountRepository.initNewScore(item1Id, item2Id);
            this.repositoryFactory.executeStatement(initCols, KEYSPACE_FIELD);
        }
    }
  
    @Override
    public void execute(Tuple input) {
        String item1Id = (String) input.getValueByField(ITEM_1_ID_FIELD); 
        String item2Id = (String) input.getValueByField(ITEM_2_ID_FIELD);
        int deltaScore = (int) input.getValueByField(DELTA_SCORE_FIELD);
        
        initPairCountTable(item1Id, item2Id);

        SimpleStatement findCurrentScore = this.pairCountRepository.getCurrentScore(item1Id, item2Id);
        ResultSet findCurrentScoreResult = this.repositoryFactory.executeStatement(findCurrentScore, KEYSPACE_FIELD);
        int currentPairCount, newPairCount;
       
        currentPairCount = (int) this.repositoryFactory.getFromRow(findCurrentScoreResult.one(), SCORE);
        newPairCount = currentPairCount + deltaScore;
        Values values = new Values(item1Id, item2Id, currentPairCount, newPairCount);
        collector.emit(values);
            
        SimpleStatement updateScoreStatement = this.pairCountRepository.updateScore(item1Id, item2Id, newPairCount);
        this.repositoryFactory.executeStatement(updateScoreStatement, KEYSPACE_FIELD);
    
        collector.ack(input);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(ITEM_1_ID_FIELD, ITEM_2_ID_FIELD, OLD_PAIR_COUNT, NEW_PAIR_COUNT));
    }
}
