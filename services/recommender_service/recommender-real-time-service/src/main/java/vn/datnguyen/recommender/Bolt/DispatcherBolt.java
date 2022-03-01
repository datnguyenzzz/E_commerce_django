package vn.datnguyen.recommender.Bolt;

import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
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
import vn.datnguyen.recommender.Repository.KeyspaceRepository;
import vn.datnguyen.recommender.Repository.RepositoryFactory;
import vn.datnguyen.recommender.utils.CustomProperties;

public class DispatcherBolt extends BaseRichBolt {
    
    private final Logger logger = LoggerFactory.getLogger(LoggerBolt.class);
    private final static CustomProperties customProperties = CustomProperties.getInstance();
    //VALUE FIELDS
    private final static String CENTRE_ID_FIELD = customProperties.getProp("CENTRE_ID_FIELD");
    private final static String EVENT_FIELD = customProperties.getProp("EVENT_FIELD");
    private final static String KEYSPACE_FIELD = customProperties.getProp("KEYSPACE_FIELD");
    private final static String NUM_NODE_REPLICAS_FIELD = customProperties.getProp("NUM_NODE_REPLICAS_FIELD");
    //CASSANDRA PROPS
    private final static String CASS_NODE = customProperties.getProp("CASS_NODE");
    private final static String CASS_PORT = customProperties.getProp("CASS_PORT");
    private final static String CASS_DATA_CENTER = customProperties.getProp("CASS_DATA_CENTER");
    //incoming event 
    private final static String avroAddItemEvent = customProperties.getProp("avroAddItemEvent");
    private final static String avroDeleteItemEvent = customProperties.getProp("avroDeleteItemEvent");
    //
    private final static String CENTRE_COORD = "centre_coord";
    private final static String CENTRE_UPPER_BOUND_RANGE_LIST = "centre_upper_bound_range_list";
    //
    private OutputCollector collector;
    private RepositoryFactory repositoryFactory;
    private IndexesCoordRepository indexesCoordRepository;
    private BoundedRingRepository boundedRingRepository;

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
        this.indexesCoordRepository = repositoryFactory.getIndexesCoordRepository();
        this.boundedRingRepository = repositoryFactory.getBoundedRingRepository();

        //create table 
        SimpleStatement createBoundedRingStatement = this.boundedRingRepository.createRowIfNotExists();
        this.repositoryFactory.executeStatement(createBoundedRingStatement, KEYSPACE_FIELD);
    }
    
    @Override
    public void execute(Tuple input) {
        Event incomeEvent = (Event) input.getValueByField(EVENT_FIELD);
        String eventType = incomeEvent.getEventType();
        int centreId = (int) input.getValueByField(CENTRE_ID_FIELD);

        SimpleStatement selectedCentreStatment = this.indexesCoordRepository.selectCentreById(centreId);
        Row selectedCentre = this.repositoryFactory.executeStatement(selectedCentreStatment, KEYSPACE_FIELD).one();

        List<Integer> centreCoord = this.repositoryFactory.getListIntegerFromRow(selectedCentre, CENTRE_COORD);
        List<Double> centreUBRangeList = this.repositoryFactory.getListDoubleFromRow(selectedCentre, CENTRE_UPPER_BOUND_RANGE_LIST);
        List<Integer> eventCoord = incomeEvent.getCoord();

        logger.info("********* DispatcherBolt **********" + incomeEvent + " with centre ID = " + centreId 
                    + " with event coord = " + incomeEvent.getCoord() 
                    + " list of upperbound range = " + centreUBRangeList
                    + " with centre coordinate = " + centreCoord);

        if (eventType.equals(avroAddItemEvent)) {
            addDataToCentre(centreId, centreCoord, centreUBRangeList, eventCoord);
        } 
        else if (eventType.equals(avroDeleteItemEvent)) {
            deleteDataFromCentre(centreId, centreCoord, centreUBRangeList, eventCoord);
        }
        collector.ack(input);
    }

    private double distance(List<Integer> a, List<Integer> b) {
        double s = 0; 
        for (int i = 0; i<a.size(); i++) {
            s += (a.get(i) - b.get(i)) * (a.get(i) - b.get(i));
        }

        return Math.sqrt(s);
    }

    private void addDataToCentre(int centreId, List<Integer> centreCoord, List<Double> centreUBRangeList, List<Integer> eventCoord) {
        //
        SortedSet<Double> sortedRangeList = new TreeSet<>();
        for (double ubRange: centreUBRangeList) {
            sortedRangeList.add(ubRange);
        }

        double dist = distance(eventCoord, centreCoord);
        SortedSet<Double> distGreaterList = sortedRangeList.tailSet(dist);

        int selectedRingId;
        int selectedRingCapacity;

        BatchStatementBuilder addDataToCentre = BatchStatement.builder(BatchType.LOGGED);
        if (distGreaterList.size() == 0) {
            // add new bounded ring
            int ringId = centreUBRangeList.size();
            double lbRange = sortedRangeList.size() == 0 
                            ? 0.0
                            : sortedRangeList.last();
            double ubRange = dist;
            
            // add new bounded ring
            SimpleStatement addNewBoundedRingStatement = 
                this.boundedRingRepository.addNewBoundedRing(ringId, centreId, lbRange, ubRange);
            
            addDataToCentre.addStatement(addNewBoundedRingStatement);
            //update centreUBList
            List<Double> tmp = List.copyOf(centreUBRangeList);
            tmp.add(ubRange);
            SimpleStatement updateUBListStatement = 
                this.indexesCoordRepository.updateUBRangeListById(centreId, tmp);
            
            addDataToCentre.addStatement(updateUBListStatement);

            selectedRingId = ringId;
            selectedRingCapacity = 0;
        } else {
        }
    }

    private void deleteDataFromCentre(int centreId, List<Integer> centreCoord, List<Double> centreUBRangeList, List<Integer> eventCoord) {
        //
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sink-bolt"));
    }
}
