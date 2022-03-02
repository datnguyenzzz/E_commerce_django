package vn.datnguyen.recommender.Bolt;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import vn.datnguyen.recommender.CassandraConnector;
import vn.datnguyen.recommender.Models.Event;
import vn.datnguyen.recommender.Repository.BoundedRingRepository;
import vn.datnguyen.recommender.Repository.IndexesCoordRepository;
import vn.datnguyen.recommender.Repository.ItemStatusRepository;
import vn.datnguyen.recommender.Repository.KeyspaceRepository;
import vn.datnguyen.recommender.Repository.RepositoryFactory;
import vn.datnguyen.recommender.utils.CustomProperties;

public class UpdateBoundedRingBolt extends BaseRichBolt {
    
    private final Logger logger = LoggerFactory.getLogger(LoggerBolt.class);
    private final static CustomProperties customProperties = CustomProperties.getInstance();
    //VALUE FIELDS
    private final static String EVENT_FIELD = customProperties.getProp("EVENT_FIELD");
    private final static String CENTRE_ID_FIELD = customProperties.getProp("CENTRE_ID_FIELD");
    private final static String RING_ID_FIELD = customProperties.getProp("RING_ID_FIELD");
    private final static String avroAddItemEvent = customProperties.getProp("avroAddItemEvent");
    private final static String avroDeleteItemEvent = customProperties.getProp("avroDeleteItemEvent");
    private final static String KEYSPACE_FIELD = customProperties.getProp("KEYSPACE_FIELD");
    private final static String NUM_NODE_REPLICAS_FIELD = customProperties.getProp("NUM_NODE_REPLICAS_FIELD");
    private final static int MIN_CAPACITY = Integer.parseInt(customProperties.getProp("MIN_CAPACITY"));
    private final static int MAX_CAPACITY = Integer.parseInt(customProperties.getProp("MAX_CAPACITY"));
    //CASSANDRA PROPS
    private final static String CASS_NODE = customProperties.getProp("CASS_NODE");
    private final static String CASS_PORT = customProperties.getProp("CASS_PORT");
    private final static String CASS_DATA_CENTER = customProperties.getProp("CASS_DATA_CENTER");
    //
    private final static String CAPACITY = "capacity";
    //
    private OutputCollector collector;
    private RepositoryFactory repositoryFactory;
    private IndexesCoordRepository indexesCoordRepository;
    private BoundedRingRepository boundedRingRepository;
    private ItemStatusRepository itemStatusRepository;

    private void launchCassandraKeyspace() {
        CassandraConnector connector = new CassandraConnector();
        connector.connect(CASS_NODE, Integer.parseInt(CASS_PORT), CASS_DATA_CENTER);
        CqlSession session = connector.getSession();

        this.repositoryFactory = new RepositoryFactory(session);
        KeyspaceRepository keyspaceRepository = this.repositoryFactory.getKeyspaceRepository();
        keyspaceRepository.createAndUseKeyspace(KEYSPACE_FIELD, Integer.parseInt(NUM_NODE_REPLICAS_FIELD));
    }
    
    @Override
    public void prepare(Map<String, Object> map, TopologyContext TopologyContext, OutputCollector collector) {
        this.collector = collector;

        launchCassandraKeyspace();

        this.indexesCoordRepository = this.repositoryFactory.getIndexesCoordRepository();
        this.boundedRingRepository = this.repositoryFactory.getBoundedRingRepository();
        this.itemStatusRepository = this.repositoryFactory.getItemStatusRepository();

        SimpleStatement createItemStatusRow = this.itemStatusRepository.createRowIfNotExists();
        this.repositoryFactory.executeStatement(createItemStatusRow, KEYSPACE_FIELD);
    }
    
    @Override
    public void execute(Tuple input) {
        Event incomeEvent = (Event) input.getValueByField(EVENT_FIELD);
        String eventType = incomeEvent.getEventType();
        String itemId = incomeEvent.getItemId();
        String clientId = incomeEvent.getClientId();
        int centreId = (int) input.getValueByField(CENTRE_ID_FIELD);
        int ringId = (int) input.getValueByField(RING_ID_FIELD);

        if (eventType.equals(avroAddItemEvent)) {
            logger.info("********* UpdateBoundedRingBolt **********: Attemp add data to ringId = " + ringId + " centreId = " + centreId);
            addDataIntoBoundedRing(itemId, clientId, centreId, ringId);
        } 
        else if (eventType.equals(avroDeleteItemEvent)) {
            logger.info("********* UpdateBoundedRingBolt **********: Attemp delete data from ringId = " + ringId + " centreId = " + centreId);
        }

        collector.ack(input);
    }

    private void addDataIntoBoundedRing(String itemId, String clientId, int centreId, int ringId) {
        SimpleStatement findBoundedRingStatement = this.boundedRingRepository.findBoundedRingById(ringId, centreId);
        ResultSet findBoundedRingResult = this.repositoryFactory.executeStatement(findBoundedRingStatement, KEYSPACE_FIELD);
        int findBoundedRingSize = findBoundedRingResult.getAvailableWithoutFetching(); 

        if (findBoundedRingSize != 1) {
            logger.warn("********* UpdateBoundedRingBolt **********: Find bounded ring by id result must equal to 1, not " + findBoundedRingSize);
        }

        Row findBoundedRing = findBoundedRingResult.one();
        int currCapacity = (int)this.repositoryFactory.getFromRow(findBoundedRing, CAPACITY); 

        if (currCapacity == MAX_CAPACITY) {
            logger.info("********* UpdateBoundedRingBolt **********: Exceed max capacity, attemp splitting bounded ring");
        } else {
            logger.info("********* UpdateBoundedRingBolt **********: Within max capacity, attemp adding to bounded ring");

            BatchStatementBuilder addDataToBoundedRing = BatchStatement.builder(BatchType.LOGGED);
            //add new item status 
        }
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sink-bolt"));
    }
}
