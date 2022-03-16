package vn.datnguyen.recommender.Bolt.ContentBased;

import java.util.ArrayList;
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
import org.apache.storm.tuple.Values;
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
    private final static String ITEM_ID_LIST_FIELD = customProperties.getProp("ITEM_ID_LIST_FIELD");
    private final static String DIST_LIST_FIELD = customProperties.getProp("DIST_LIST_FIELD");
    private final static String EVENT_ID_FIELD = customProperties.getProp("EVENT_ID_FIELD");
    //stream 
    private final static String INDIVIDUAL_KNN_ALGORITHM_STREAM = customProperties.getProp("INDIVIDUAL_KNN_ALGORITHM_STREAM");
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
    private Map<String, List<Integer> > itemPropertiesTable;

    private void launchCassandraKeyspace() {
        CassandraConnector connector = new CassandraConnector();
        connector.connect(CASS_NODE, Integer.parseInt(CASS_PORT), CASS_DATA_CENTER);
        CqlSession session = connector.getSession();

        this.repositoryFactory = new RepositoryFactory(session);
        KeyspaceRepository keyspaceRepository = this.repositoryFactory.getKeyspaceRepository();
        keyspaceRepository.createAndUseKeyspace(KEYSPACE_FIELD, Integer.parseInt(NUM_NODE_REPLICAS_FIELD));
    }

    private long distance(List<Integer> a, List<Integer> b) {
        long s = 0; 
        for (int i = 0; i<a.size(); i++) {
            s += (a.get(i) - b.get(i)) * (a.get(i) - b.get(i));
        }

        return s;
    } 

    @Override
    public void prepare(Map<String, Object> map, TopologyContext TopologyContext, OutputCollector collector) {
        this.collector = collector;
        this.knnFactor = 0; 

        launchCassandraKeyspace();
        this.itemStatusRepository = this.repositoryFactory.getItemStatusRepository();
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public void execute(Tuple input) {

        List<Integer> eventCoord = (List<Integer>) input.getValueByField(EVENT_COORD_FIELD);
        String eventId = (String) input.getValueByField(EVENT_ID_FIELD);
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

        Comparator<String> customComparator = (t1, t2) -> {
            List<Integer> item1Properties = itemPropertiesTable.get(t1);
            List<Integer> item2Properties = itemPropertiesTable.get(t2);
    
            long dist1 = distance(item1Properties, this.eventCoord);
            long dist2 = distance(item2Properties, this.eventCoord);
    
            int cmp = (dist1 < dist2) 
                        ? 1
                        : -1; 
            return cmp;
        };

        this.pq = new PriorityQueue<>(customComparator);
        this.itemPropertiesTable = new HashMap<>();

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

            itemPropertiesTable.put(itemId, itemProperties);
        }
        
        // build pq
        for (Row r: findAllItemInside) {
            String itemId = (String) this.repositoryFactory.getFromRow(r, ITEM_ID);
            pq.add(itemId);

            logger.info("********* KnnBolt ********** PQ before erase - " + pq);
            if (pq.size() > this.knnFactor) {
                pq.poll();
            }
            logger.info("********* KnnBolt ********** PQ after erase - " + pq);
        }
        
        //emit tuple

        List<String> itemIdList = new ArrayList<>();
        List<Double> distList = new ArrayList<>();

        while (pq.size() > 0) {
            String itemId = pq.poll();
            long dist = distance(eventCoord, itemPropertiesTable.get(itemId));
            itemIdList.add(itemId);
            distList.add(Math.sqrt(dist));
        }

        collector.emit(INDIVIDUAL_KNN_ALGORITHM_STREAM, new Values(eventId, eventCoord, centreId, ringId.toString(), itemIdList, distList));
        collector.ack(input);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(INDIVIDUAL_KNN_ALGORITHM_STREAM, new Fields(EVENT_ID_FIELD, EVENT_COORD_FIELD, CENTRE_ID_FIELD, RING_ID_FIELD, ITEM_ID_LIST_FIELD, DIST_LIST_FIELD));
    }
}
