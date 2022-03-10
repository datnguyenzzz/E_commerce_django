package vn.datnguyen.recommender.Bolt.ContentBased;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.UUID;

import com.datastax.oss.driver.api.core.CqlSession;
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
import vn.datnguyen.recommender.Repository.ItemStatusRepository;
import vn.datnguyen.recommender.Repository.KeyspaceRepository;
import vn.datnguyen.recommender.Repository.RepositoryFactory;
import vn.datnguyen.recommender.utils.CustomProperties;

public class KnnBolt extends BaseRichBolt {
    
    private final Logger logger = LoggerFactory.getLogger(KnnBolt.class);
    private final static CustomProperties customProperties = CustomProperties.getInstance();
    //field tuple
    private final static String EVENT_COORD_FIELD = customProperties.getProp("EVENT_COORD_FIELD");
    private final static String CENTRE_ID_FIELD = customProperties.getProp("CENTRE_ID_FIELD");
    private final static String RING_ID_FIELD = customProperties.getProp("RING_ID_FIELD");
    private final static String KNN_FACTOR_FIELD = customProperties.getProp("KNN_FACTOR_FIELD");
    //WEIGHT VALUEs
    private final static String KEYSPACE_FIELD = customProperties.getProp("KEYSPACE_FIELD");
    private final static String NUM_NODE_REPLICAS_FIELD = customProperties.getProp("NUM_NODE_REPLICAS_FIELD");
    //CASSANDRA PROPS
    private final static String CASS_NODE = customProperties.getProp("CASS_NODE");
    private final static String CASS_PORT = customProperties.getProp("CASS_PORT");
    private final static String CASS_DATA_CENTER = customProperties.getProp("CASS_DATA_CENTER");
    //table col
    private final static String ITEM_ID = "item_id"; // string
    private final static String VECTOR_PROPERTIES = "vector_properties"; //list<int>
    //
    private OutputCollector collector;
    private int knnFactor;
    private List<Integer> eventCoord;
    private RepositoryFactory repositoryFactory;
    private ItemStatusRepository itemStatusRepository;
    private PriorityQueue<String> pq;
    //fixed per ring
    private Map<String, List<Integer> > itemPropertiesTable;

    private void launchCassandraKeyspace() {
        CassandraConnector connector = new CassandraConnector();
        connector.connect(CASS_NODE, Integer.parseInt(CASS_PORT), CASS_DATA_CENTER);
        CqlSession session = connector.getSession();

        this.repositoryFactory = new RepositoryFactory(session);
        KeyspaceRepository keyspaceRepository = this.repositoryFactory.getKeyspaceRepository();
        keyspaceRepository.createAndUseKeyspace(KEYSPACE_FIELD, Integer.parseInt(NUM_NODE_REPLICAS_FIELD));
    }

    private Comparator<String > customeComparator = (t1, t2) -> {
        return 0;
    };

    @Override
    public void prepare(Map<String, Object> map, TopologyContext TopologyContext, OutputCollector collector) {
        this.collector = collector;
        this.knnFactor = 0; 
        this.itemPropertiesTable = new HashMap<>();

        launchCassandraKeyspace();
        this.itemStatusRepository = this.repositoryFactory.getItemStatusRepository();
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public void execute(Tuple input) {

        List<Integer> eventCoord = (List<Integer>) input.getValueByField(EVENT_COORD_FIELD);
        int centreId = (int) input.getValueByField(CENTRE_ID_FIELD);
        UUID ringId = UUID.fromString((String) input.getValueByField(RING_ID_FIELD));
        int knnFactor = (int) input.getValueByField(KNN_FACTOR_FIELD);

        logger.info("********* KnnBolt **********: Received signal for event coord = " + eventCoord
                    + " centreId = " + centreId
                    + " ringId = " + ringId
                    + " knn factor = " + knnFactor);

        // init individual value
        this.knnFactor = knnFactor;
        this.eventCoord = eventCoord;
        this.pq = new PriorityQueue<>(customeComparator);

        //find all item inside ring 
        SimpleStatement findAllItemInsideStatement = 
            this.itemStatusRepository.findAllByRingId(ringId, centreId);
        List<Row> findAllItemInside = 
            this.repositoryFactory.executeStatement(findAllItemInsideStatement, KEYSPACE_FIELD)
                .all();

        // build map item properties table
        for (Row r: findAllItemInside) {
            String itemId = (String) this.repositoryFactory.getFromRow(r, ITEM_ID);
            List<Integer> itemProperties = this.repositoryFactory.getListIntegerFromRow(r, VECTOR_PROPERTIES);

            if (!(itemPropertiesTable.containsKey(itemId)) 
               || itemPropertiesTable.get(itemId) != itemProperties) {
                itemPropertiesTable.put(itemId, itemProperties);
            }
        }
        
        collector.ack(input);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sink-bolt"));
    }
}
