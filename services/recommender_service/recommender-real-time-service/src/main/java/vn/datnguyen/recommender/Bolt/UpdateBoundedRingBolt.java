package vn.datnguyen.recommender.Bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.uuid.Uuids;

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
    private final static String DISTANCE_TO_CENTRE = "distance_to_centre"; 
    private final static String CENTRE_UPPER_BOUND_RANGE_LIST = "centre_upper_bound_range_list";
    private final static String UPPER_BOUND_RANGE = "upper_bound_range";
    private final static String LOWER_BOUND_RANGE = "lower_bound_range";
    private final static String CENTRE_COORD = "centre_coord";
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
        String ringIdStr = (String) input.getValueByField(RING_ID_FIELD);

        int centreId = (int) input.getValueByField(CENTRE_ID_FIELD);
        UUID ringId = UUID.fromString(ringIdStr);

        if (eventType.equals(avroAddItemEvent)) {
            logger.info("********* UpdateBoundedRingBolt **********: Attemp add data to ringId = " + ringId + " centreId = " + centreId);
            addDataIntoBoundedRing(itemId, clientId, incomeEvent.getCoord(), centreId, ringId);
        } 
        else if (eventType.equals(avroDeleteItemEvent)) {
            logger.info("********* UpdateBoundedRingBolt **********: Attemp delete data from ringId = " + ringId + " centreId = " + centreId);
        }

        collector.ack(input);
    }

    private double findMedianFromDataSet(List<Row> getAllDataInRing) {
        SortedSet<Double> dist = new TreeSet<Double>();

        for (Row r: getAllDataInRing) {
            double d = (Double)this.repositoryFactory.getFromRow(r, DISTANCE_TO_CENTRE);
            dist.add(d);
        }

        Double[] distArray = new Double[dist.size()];
        distArray = dist.toArray(distArray);

        return distArray[dist.size()/2];
    }

    private double distance(List<Integer> a, List<Integer> b) {
        double s = 0; 
        for (int i = 0; i<a.size(); i++) {
            s += (a.get(i) - b.get(i)) * (a.get(i) - b.get(i));
        }

        return Math.sqrt(s);
    }

    private void addDataIntoBoundedRing(String itemId, String clientId, List<Integer> eventCoord, int centreId, UUID ringId) {
        //bouded ring 
        SimpleStatement findBoundedRingStatement = this.boundedRingRepository.findBoundedRingById(ringId, centreId);
        ResultSet findBoundedRingResult = this.repositoryFactory.executeStatement(findBoundedRingStatement, KEYSPACE_FIELD);
        int findBoundedRingSize = findBoundedRingResult.getAvailableWithoutFetching(); 

        if (findBoundedRingSize != 1) {
            logger.warn("********* UpdateBoundedRingBolt **********: Find bounded ring by id result must equal to 1, not " + findBoundedRingSize);
        }

        Row findBoundedRing = findBoundedRingResult.one();
        int currCapacity = (int)this.repositoryFactory.getFromRow(findBoundedRing, CAPACITY); 
        // center 
        SimpleStatement getCentreStatement = 
                this.indexesCoordRepository.selectCentreById(centreId);
        ResultSet getCentreResult = 
            this.repositoryFactory.executeStatement(getCentreStatement, KEYSPACE_FIELD);
        if (getCentreResult.getAvailableWithoutFetching() != 1) {
            logger.warn("********* UpdateBoundedRingBolt **********: Find centre by id result must equal to 1, not " + getCentreResult.getAvailableWithoutFetching());
        }
        Row getCentre = getCentreResult.one();

        BatchStatementBuilder addDataToBoundedRing = BatchStatement.builder(BatchType.LOGGED);

        if (currCapacity == MAX_CAPACITY) {
            logger.info("********* UpdateBoundedRingBolt **********: Exceed max capacity, attemp splitting bounded ring");
            //get all data in bounded ring 
            SimpleStatement getAllDataInRingStatement = 
                this.itemStatusRepository.findAllByRingId(ringId, centreId);
            List<Row> getAllDataInRing = 
                this.repositoryFactory.executeStatement(getAllDataInRingStatement, KEYSPACE_FIELD).all();

            //get UBRANGE list 
            List<Double> centreUBRangeList = 
                this.repositoryFactory.getListDoubleFromRow(getCentre, CENTRE_UPPER_BOUND_RANGE_LIST);

            SortedSet<Double> centreUBRangeSet = new TreeSet<>();
            for (double ubRange: centreUBRangeList) {
                centreUBRangeSet.add(ubRange);
            }

            //logic
            double medianRangeInBoundedRing = findMedianFromDataSet(getAllDataInRing);
            double oldMaxRange = Double.MIN_VALUE;
            int newRingCapacity = 0;
            int oldCapacity = getAllDataInRing.size();
            UUID newRingId = Uuids.random();
            //update item status 
            for (Row r: getAllDataInRing) {
                double d = (Double)this.repositoryFactory.getFromRow(r, DISTANCE_TO_CENTRE);
                if (d <= medianRangeInBoundedRing) {
                    oldMaxRange = Math.max(oldMaxRange, d);
                    continue;
                }

                newRingCapacity += 1;
                SimpleStatement changeToNewBoundedRingStatement = 
                    this.itemStatusRepository.updateItemStatusRingId(itemId, clientId, newRingId);
                addDataToBoundedRing.addStatement(changeToNewBoundedRingStatement);
            }
            //update bounded ring
            double lbRange = (double) this.repositoryFactory.getFromRow(findBoundedRing, LOWER_BOUND_RANGE);
            double ubRange = (double) this.repositoryFactory.getFromRow(findBoundedRing, UPPER_BOUND_RANGE);
            SimpleStatement updateOldRingRangeStatement = 
                this.boundedRingRepository.updateBoundedRingRange(ringId, centreId, lbRange, oldMaxRange);
            SimpleStatement updateOldRingCapacityStatement = 
                this.boundedRingRepository.updateBoundedRingCapacityById(ringId, centreId, oldCapacity - newRingCapacity);
            
            SimpleStatement insertNewRingStatement = 
                this.boundedRingRepository.addNewBoundedRing(newRingId, centreId, oldMaxRange, ubRange);
            SimpleStatement updateNewRingCapacityStatement = 
                this.boundedRingRepository.updateBoundedRingCapacityById(newRingId, centreId, newRingCapacity);
            
            addDataToBoundedRing.addStatement(updateOldRingRangeStatement)
                .addStatement(updateOldRingCapacityStatement)
                .addStatement(insertNewRingStatement)
                .addStatement(updateNewRingCapacityStatement);
            
            //update centre
            centreUBRangeSet.add(oldMaxRange);
            List<Double> centreUBRangeArrayList = new ArrayList<Double>(centreUBRangeSet);
            SimpleStatement updateUBListStatement = 
                this.indexesCoordRepository.updateUBRangeListById(centreId, centreUBRangeArrayList);
            
            addDataToBoundedRing.addStatement(updateUBListStatement);

        } else {
            logger.info("********* UpdateBoundedRingBolt **********: Within max capacity, attemp adding to bounded ring");
            List<Integer> centreCoord = 
                this.repositoryFactory.getListIntegerFromRow(getCentre, CENTRE_COORD);
            
            double dist = distance(eventCoord, centreCoord);

            //add new item status 
            SimpleStatement addNewItemStatusStatement = 
                this.itemStatusRepository.addNewItemStatus(itemId, clientId, ringId, centreId, dist);
            
            //update curr capacity 
            SimpleStatement increaseCapacityStatement =
                this.boundedRingRepository.updateBoundedRingCapacityById(ringId, centreId, currCapacity+1);
            
            addDataToBoundedRing.addStatement(addNewItemStatusStatement)
                .addStatement(increaseCapacityStatement);

        }

        this.repositoryFactory.executeStatement(addDataToBoundedRing.build(), KEYSPACE_FIELD);

    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sink-bolt"));
    }
}
