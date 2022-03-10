package vn.datnguyen.recommender.Bolt.ContentBased;

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
import vn.datnguyen.recommender.Bolt.LoggerBolt;
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
    private final static String ITEM_ID = "item_id"; // string
    private final static String RING_ID = "ring_id";
    private final static String ADD_BY_CLIENT_ID = "add_by_client_id"; // string
    private final static String DISTANCE_TO_CENTRE = "distance_to_centre"; 
    private final static String CENTRE_UPPER_BOUND_RANGE_LIST = "centre_upper_bound_range_list";
    private final static String UPPER_BOUND_RANGE = "upper_bound_range";
    private final static String LOWER_BOUND_RANGE = "lower_bound_range";
    private final static String CENTRE_COORD = "centre_coord";
    private final static String CAPACITY = "capacity";
    private final static String VECTOR_PROPERTIES = "vector_properties"; //list<int>
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
            deleteDataFromBoundedRing(itemId, clientId, incomeEvent.getCoord(), centreId, ringId);
        }

        collector.ack(input);
    }

    private void updateBoundedRingRange(UUID ringId, int centreId, double newLBRange, double newUBRange, int newCapacity) {

        //Delete 
        SimpleStatement deleteCurrRowStatement = 
            this.boundedRingRepository.deleteBoundedRingById(ringId, centreId);
        this.repositoryFactory.executeStatement(deleteCurrRowStatement, KEYSPACE_FIELD);

        //add new 
        SimpleStatement addNewRowStatement = 
            this.boundedRingRepository.addNewBoundedRing(ringId, centreId, newLBRange, newUBRange, newCapacity);

        this.repositoryFactory.executeStatement(addNewRowStatement, KEYSPACE_FIELD);
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

    private List<SimpleStatement> splitBoundedRing(int centreId, UUID ringId) {

        List<SimpleStatement> splitBoundedRing = new ArrayList<SimpleStatement>();
        
        //bouded ring 
        SimpleStatement findBoundedRingStatement = this.boundedRingRepository.findBoundedRingById(ringId, centreId);
        ResultSet findBoundedRingResult = this.repositoryFactory.executeStatement(findBoundedRingStatement, KEYSPACE_FIELD);
        int findBoundedRingSize = findBoundedRingResult.getAvailableWithoutFetching(); 

        if (findBoundedRingSize != 1) {
            logger.warn("********* UpdateBoundedRingBolt **********: Find bounded ring by id result must equal to 1, not " + findBoundedRingSize);
        }

        Row findBoundedRing = findBoundedRingResult.one();
        // center 
        SimpleStatement getCentreStatement = 
                this.indexesCoordRepository.selectCentreById(centreId);
        ResultSet getCentreResult = 
            this.repositoryFactory.executeStatement(getCentreStatement, KEYSPACE_FIELD);
        if (getCentreResult.getAvailableWithoutFetching() != 1) {
            logger.warn("********* UpdateBoundedRingBolt **********: Find centre by id result must equal to 1, not " + getCentreResult.getAvailableWithoutFetching());
        }
        Row getCentre = getCentreResult.one();

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
        logger.info("********* UpdateBoundedRingBolt **********: Distance threshold for splitting = " + medianRangeInBoundedRing);

        double oldMaxRange = Double.MIN_VALUE;
        int newRingCapacity = 0;
        int oldCapacity = getAllDataInRing.size();
        UUID newRingId = Uuids.random();
        //update item status 
        for (Row r: getAllDataInRing) {
            double d = (Double)this.repositoryFactory.getFromRow(r, DISTANCE_TO_CENTRE);
            List<Integer> itemProperties = this.repositoryFactory.getListIntegerFromRow(r, VECTOR_PROPERTIES);
            if (d <= medianRangeInBoundedRing) {
                oldMaxRange = Math.max(oldMaxRange, d);
                continue;
            }

            String itemId = (String) this.repositoryFactory.getFromRow(r, ITEM_ID);
            String clientId = (String) this.repositoryFactory.getFromRow(r, ADD_BY_CLIENT_ID);
            newRingCapacity += 1;
            SimpleStatement changeToNewBoundedRingStatement = 
                this.itemStatusRepository.addNewItemStatus(itemId, clientId, newRingId, centreId, d, itemProperties);
            SimpleStatement deleteCurrDataFromRingStatement = 
                this.itemStatusRepository.deleteItemStatus(itemId, ringId, centreId);
            splitBoundedRing.add(deleteCurrDataFromRingStatement);
            splitBoundedRing.add(changeToNewBoundedRingStatement);
        }
        //update bounded ring
        double lbRange = (double) this.repositoryFactory.getFromRow(findBoundedRing, LOWER_BOUND_RANGE);
        double ubRange = (double) this.repositoryFactory.getFromRow(findBoundedRing, UPPER_BOUND_RANGE);

        updateBoundedRingRange(ringId, centreId, lbRange, oldMaxRange, oldCapacity - newRingCapacity);

        SimpleStatement insertNewRingStatement = 
            this.boundedRingRepository.addNewBoundedRing(newRingId, centreId, oldMaxRange, ubRange, newRingCapacity);
            
        splitBoundedRing.add(insertNewRingStatement);
            
        //update centre
        centreUBRangeSet.add(oldMaxRange);
        List<Double> centreUBRangeArrayList = new ArrayList<Double>(centreUBRangeSet);
        SimpleStatement updateUBListStatement = 
            this.indexesCoordRepository.updateUBRangeListById(centreId, centreUBRangeArrayList);
            
        splitBoundedRing.add(updateUBListStatement);
        
        return splitBoundedRing;
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
        double currLBRange = (double) this.repositoryFactory.getFromRow(findBoundedRing, LOWER_BOUND_RANGE);
        double currUBRange = (double) this.repositoryFactory.getFromRow(findBoundedRing, UPPER_BOUND_RANGE);
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

        logger.info("********* UpdateBoundedRingBolt **********: attemp adding to bounded ring");
        List<Integer> centreCoord = 
            this.repositoryFactory.getListIntegerFromRow(getCentre, CENTRE_COORD);
            
        double dist = distance(eventCoord, centreCoord);

        //add new item status 
        SimpleStatement addNewItemStatusStatement = 
            this.itemStatusRepository.addNewItemStatus(itemId, clientId, ringId, centreId, dist, eventCoord);
            
        //update curr capacity 
        updateBoundedRingRange(ringId, centreId, currLBRange, currUBRange, currCapacity+1);
            
        addDataToBoundedRing.addStatement(addNewItemStatusStatement);
        
        this.repositoryFactory.executeStatement(addDataToBoundedRing.build(), KEYSPACE_FIELD);

        // split if exceed range
        if (currCapacity == MAX_CAPACITY) {
            BatchStatementBuilder splitBoundedRingBatch = BatchStatement.builder(BatchType.LOGGED);
            List<SimpleStatement> splitBoundedRingListStatements = splitBoundedRing(centreId, ringId);
            for (SimpleStatement statement: splitBoundedRingListStatements) {
                splitBoundedRingBatch.addStatement(statement);
            }
            this.repositoryFactory.executeStatement(splitBoundedRingBatch.build(), KEYSPACE_FIELD);
        }

    }
    
    private List<SimpleStatement> mergeBoundedRings(int centreId, UUID ringId) {

        List<SimpleStatement> mergeBoundedRingsList = new ArrayList<>();

        //centre 
        SimpleStatement selectedCentreStatement = this.indexesCoordRepository.selectCentreById(centreId);
        ResultSet selectedCentreResult = 
            this.repositoryFactory.executeStatement(selectedCentreStatement, KEYSPACE_FIELD);
        if (selectedCentreResult.getAvailableWithoutFetching() > 1) {
            logger.warn("********* UpdateBoundedRingBolt **********: Find centre by id result must equal to 1, not " + selectedCentreResult.getAvailableWithoutFetching());
        }
        Row selectedCentre = selectedCentreResult.one();
        List<Double> ubRangeList = 
            this.repositoryFactory.getListDoubleFromRow(selectedCentre, CENTRE_UPPER_BOUND_RANGE_LIST);
        
        if (ubRangeList.size() == 1) {
            return mergeBoundedRingsList;
        }

        SortedSet<Double> ubRangeSet = new TreeSet<>();
        for (double ubRange: ubRangeList) {
            ubRangeSet.add(ubRange);
        }

        //current bounded ring 
        SimpleStatement selectedBoundedRingStatement =
            this.boundedRingRepository.findBoundedRingById(ringId, centreId);
        ResultSet selectedBoundedRingResult = 
            this.repositoryFactory.executeStatement(selectedBoundedRingStatement, KEYSPACE_FIELD);
        if (selectedBoundedRingResult.getAvailableWithoutFetching() > 1) {
            logger.warn("********* UpdateBoundedRingBolt **********: Find ring by id result must equal to 1, not " + selectedBoundedRingResult.getAvailableWithoutFetching());
        }
        Row selectedBoundedRing = selectedBoundedRingResult.one();
        UUID selectedBoundedRingId = ringId;
        double selectedRingLBRange = 
            (double) this.repositoryFactory.getFromRow(selectedBoundedRing, LOWER_BOUND_RANGE);
        double selectedRingUBRange = 
            (double) this.repositoryFactory.getFromRow(selectedBoundedRing, UPPER_BOUND_RANGE);
        int selectedRingCapacity = 
            (int) this.repositoryFactory.getFromRow(selectedBoundedRing, CAPACITY);

        // find partner ring
        
        double partnerRingWithUBRange=-1;
        if (selectedRingLBRange == 0.0) {
            // 0 -- 1
            partnerRingWithUBRange = ubRangeList.get(1);
        } else {
            // i -- i-1
            for (double ubRange: ubRangeList) {
                if (ubRange == selectedRingLBRange) {
                    partnerRingWithUBRange = ubRange;
                    break;
                }
            }
        }

        if (partnerRingWithUBRange == -1) {
            logger.warn("********* UpdateBoundedRingBolt **********: can not find previous of ringId " + ringId);
        }

        SimpleStatement allRingInCentreStatement = this.boundedRingRepository.findAllBoundedRingInCentre(centreId);   
        List<Row> allRingInCentre = 
            this.repositoryFactory.executeStatement(allRingInCentreStatement, KEYSPACE_FIELD)
                .all();
        
        UUID partnerRingId = null;
        double partnerRingLBRange=-1;
        double partnerRingUBRange=-1;
        int partnerRingCapacity=0;
        for (Row r: allRingInCentre) {
            double ubRange = (double) this.repositoryFactory.getFromRow(r, UPPER_BOUND_RANGE);
            UUID id = (UUID) this.repositoryFactory.getFromRow(r, RING_ID);
            if (ubRange == partnerRingWithUBRange) {
                partnerRingId = id;
                partnerRingCapacity = (int) this.repositoryFactory.getFromRow(r, CAPACITY);
                partnerRingLBRange = (double) this.repositoryFactory.getFromRow(r, LOWER_BOUND_RANGE);
                partnerRingUBRange = (double) this.repositoryFactory.getFromRow(r, UPPER_BOUND_RANGE);
                break;
            }
        }

        if (partnerRingId == null) {
            logger.warn("********* UpdateBoundedRingBolt **********: can not find partner of ringId " + ringId);
        }

        //delete partner ring
        SimpleStatement deletePartnerRing = 
            this.boundedRingRepository.deleteBoundedRingById(partnerRingId, centreId);
        mergeBoundedRingsList.add(deletePartnerRing);

        //move item from partner ring to selected ring
        SimpleStatement allDataInPartnerRingStatement = 
            this.itemStatusRepository.findAllByRingId(partnerRingId, centreId);
        List<Row> allDataInPartnerRing =
            this.repositoryFactory.executeStatement(allDataInPartnerRingStatement, KEYSPACE_FIELD)
                .all();
        for (Row r: allDataInPartnerRing) {
            String itemId = (String) this.repositoryFactory.getFromRow(r, ITEM_ID);
            String clientId = (String) this.repositoryFactory.getFromRow(r, ADD_BY_CLIENT_ID);
            double dist = (double) this.repositoryFactory.getFromRow(r, DISTANCE_TO_CENTRE);
            List<Integer> itemProperties = this.repositoryFactory.getListIntegerFromRow(r, VECTOR_PROPERTIES);

            SimpleStatement removeDataFromBoundedRing = 
                this.itemStatusRepository.deleteItemStatus(itemId, partnerRingId, centreId);
            SimpleStatement addDataIntoBoundedRing = 
                this.itemStatusRepository.addNewItemStatus(itemId, clientId, selectedBoundedRingId, centreId, dist, itemProperties);

            mergeBoundedRingsList.add(removeDataFromBoundedRing);
            mergeBoundedRingsList.add(addDataIntoBoundedRing);
        }
        //update props of selected ring
        int newCapacity = selectedRingCapacity + partnerRingCapacity;

        double newLBRange = Math.min(selectedRingLBRange, partnerRingLBRange);
        double newUBRange = Math.max(selectedRingUBRange, partnerRingUBRange);
        double oldUBRange = Math.min(selectedRingUBRange, partnerRingUBRange);

        updateBoundedRingRange(selectedBoundedRingId, centreId, newLBRange, newUBRange, newCapacity);

        // update UBrange list of centre
        ubRangeSet.remove(partnerRingUBRange);
        if (ubRangeSet.contains(oldUBRange)) {
            ubRangeSet.remove(oldUBRange);
        }
        ubRangeSet.add(newUBRange);
        List<Double> tempUBRangeList = new ArrayList<>(ubRangeSet);
        SimpleStatement updateUBRangeList = 
            this.indexesCoordRepository.updateUBRangeListById(centreId, tempUBRangeList);
        mergeBoundedRingsList.add(updateUBRangeList);

        return mergeBoundedRingsList;
    }

    private void deleteDataFromBoundedRing(String itemId, String clientId, List<Integer> eventCoord, int centreId, UUID ringId) {
        //batch statement 
        BatchStatementBuilder deleteDataFromBoundedRing = BatchStatement.builder(BatchType.LOGGED);
        //bounded ring
        SimpleStatement selectedBoundedRingStatement = 
            this.boundedRingRepository.findBoundedRingById(ringId, centreId);
        ResultSet selectedBoundedRingResult = 
            this.repositoryFactory.executeStatement(selectedBoundedRingStatement, KEYSPACE_FIELD);
        
        if (selectedBoundedRingResult.getAvailableWithoutFetching() != 1) {
            logger.warn("********* UpdateBoundedRingBolt **********: Find ring by id result must equal to 1, not " + selectedBoundedRingResult.getAvailableWithoutFetching());
        }

        Row selectedBoundedRing = selectedBoundedRingResult.one();
        int currentCapacity = (int) this.repositoryFactory.getFromRow(selectedBoundedRing, CAPACITY);
        double currLBRange = (double) this.repositoryFactory.getFromRow(selectedBoundedRing, LOWER_BOUND_RANGE);
        double currUBRange = (double) this.repositoryFactory.getFromRow(selectedBoundedRing, UPPER_BOUND_RANGE);

        // delete item from bouneded ring
        SimpleStatement deleteItemStatus = 
            this.itemStatusRepository.deleteItemStatus(itemId, ringId, centreId);
        deleteDataFromBoundedRing.addStatement(deleteItemStatus);

        // update capacity 
        updateBoundedRingRange(ringId, centreId, currLBRange, currUBRange, currentCapacity-1);

        this.repositoryFactory.executeStatement(deleteDataFromBoundedRing.build(), KEYSPACE_FIELD);

        if (currentCapacity == MIN_CAPACITY) {
            BatchStatementBuilder mergeBoundedRingsBatch = BatchStatement.builder(BatchType.LOGGED);
            List<SimpleStatement> mergeBoundedRings = mergeBoundedRings(centreId, ringId); 
            for (SimpleStatement statement: mergeBoundedRings) {
                mergeBoundedRingsBatch.addStatement(statement);
            }
            this.repositoryFactory.executeStatement(mergeBoundedRingsBatch.build(), KEYSPACE_FIELD);

            // split if exceed MAX CAP
            selectedBoundedRingStatement = 
                this.boundedRingRepository.findBoundedRingById(ringId, centreId);
            selectedBoundedRingResult =  
                this.repositoryFactory.executeStatement(selectedBoundedRingStatement, KEYSPACE_FIELD);
            
            if (selectedBoundedRingResult.getAvailableWithoutFetching() != 1) {
                logger.warn("********* UpdateBoundedRingBolt **********: Find ring by id result must equal to 1, not " + selectedBoundedRingResult.getAvailableWithoutFetching());
            }

            selectedBoundedRing = selectedBoundedRingResult.one();
            currentCapacity = (int) this.repositoryFactory.getFromRow(selectedBoundedRing, CAPACITY);
            if (currentCapacity > MAX_CAPACITY) {
                BatchStatementBuilder splitBoundedRingBatch = BatchStatement.builder(BatchType.LOGGED);
                List<SimpleStatement> splitBoundedRingListStatements = splitBoundedRing(centreId, ringId);
                for (SimpleStatement statement: splitBoundedRingListStatements) {
                    splitBoundedRingBatch.addStatement(statement);
                }
                this.repositoryFactory.executeStatement(splitBoundedRingBatch.build(), KEYSPACE_FIELD);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sink-bolt"));
    }
}
