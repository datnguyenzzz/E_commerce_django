package vn.datnguyen.recommender.Bolt.ContentBased;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
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
import vn.datnguyen.recommender.Repository.BoundedRingRepository;
import vn.datnguyen.recommender.Repository.IndexesCoordRepository;
import vn.datnguyen.recommender.Repository.KeyspaceRepository;
import vn.datnguyen.recommender.Repository.RepositoryFactory;
import vn.datnguyen.recommender.utils.CustomProperties;

public class RecommendForItemContentBased extends BaseRichBolt {
    
    private final Logger logger = LoggerFactory.getLogger(RecommendForItemContentBased.class);
    private final static CustomProperties customProperties = CustomProperties.getInstance();
    //STREAM
    private final static String INDIVIDUAL_BOUNDED_RING_HANDLER_STREAM = customProperties.getProp("INDIVIDUAL_BOUNDED_RING_HANDLER_STREAM");
    private final static String AGGREGATE_BOUNDED_RINGS_STREAM = customProperties.getProp("AGGREGATE_BOUNDED_RINGS_STREAM");
    //VALUE FIELDS
    private final static String EVENT_FIELD = customProperties.getProp("EVENT_FIELD");
    private final static String CENTRE_ID_FIELD = customProperties.getProp("CENTRE_ID_FIELD");
    private final static String RING_ID_FIELD = customProperties.getProp("RING_ID_FIELD");
    private final static String KNN_FACTOR_FIELD = customProperties.getProp("KNN_FACTOR_FIELD");
    private final static String RING_LIST_FIELD = customProperties.getProp("RING_LIST_FIELD");
    private final static String CENTRE_LIST_FIELD = customProperties.getProp("CENTRE_LIST_FIELD");
    //WEIGHT VALUEs
    private final static String KEYSPACE_FIELD = customProperties.getProp("KEYSPACE_FIELD");
    private final static String NUM_NODE_REPLICAS_FIELD = customProperties.getProp("NUM_NODE_REPLICAS_FIELD");
    //CASSANDRA PROPS
    private final static String CASS_NODE = customProperties.getProp("CASS_NODE");
    private final static String CASS_PORT = customProperties.getProp("CASS_PORT");
    private final static String CASS_DATA_CENTER = customProperties.getProp("CASS_DATA_CENTER");
    //table col
    private final static String CENTRE_ID = "centre_id";
    private final static String CENTRE_COORD = "centre_coord";
    private final static String CENTRE_UPPER_BOUND_RANGE_LIST = "centre_upper_bound_range_list";
    //
    private OutputCollector collector;
    private int K, A, B;
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
        this.indexesCoordRepository = this.repositoryFactory.getIndexesCoordRepository();
        this.boundedRingRepository = this.repositoryFactory.getBoundedRingRepository();
    }

    private double dist(List<Integer> a, List<Integer> b) {
        double s = 0;
        for (int i=0; i<a.size(); i++) {
            s += (a.get(i) - b.get(i)) * (a.get(i) - b.get(i));
        }
        return Math.sqrt(s);
    }

    /*
        STUPID AS FUCK !!!!!!
        BUT TOO LAZY TO IMPROVE ALGORITHM ^_^
    */
    private UUID findCorrectspondBoundedRing(List<Row> allBoundedRingWithin, double ubRange) {
        return null;
    }

    private List<ImmutablePair<Integer, UUID> > findPotentialBoundedRings(List<Integer> eventCoord, int K, int A, int B) {
        
        List<ImmutablePair<Integer, UUID> > result = new ArrayList<>();

        Comparator<ImmutableTriple<Double, Integer, UUID> > customCompare = (tuple1, tuple2) -> {
            double cmp = tuple2.getLeft() - tuple1.getLeft();
            if (cmp > 0) {
                return 1; 
            } 
            else if (cmp < 0) {
                return -1;
            }
            return 0;
        };

        PriorityQueue<ImmutableTriple<Double, Integer, UUID> > pq = new PriorityQueue<>(customCompare);

        //get all centre and rings within
        SimpleStatement allCentreStatement = this.indexesCoordRepository.selectAllCentre();
        List<Row> allCentre = 
            this.repositoryFactory.executeStatement(allCentreStatement, KEYSPACE_FIELD)
                .all();

        for (Row r: allCentre) {
            //centre 
            int centreId = (int) this.repositoryFactory.getFromRow(r, CENTRE_ID); 
            List<Integer> centreCoord = this.repositoryFactory.getListIntegerFromRow(r, CENTRE_COORD);
            List<Double> ringUBRangeList = this.repositoryFactory.getListDoubleFromRow(r, CENTRE_UPPER_BOUND_RANGE_LIST);
            SortedSet<Double> ringUBRangeSet = new TreeSet<>(ringUBRangeList);
            double distFromCentreId = dist(centreCoord, eventCoord);

            //bounded ring within
            SimpleStatement allBoundedRingWithinStatement = 
                this.boundedRingRepository.findAllBoundedRingInCentre(centreId);
            List<Row> allBoundedRingWithin =
                this.repositoryFactory.executeStatement(allBoundedRingWithinStatement, KEYSPACE_FIELD)
                    .all();

            //find lower bound in UBRange > distFromCentre
            SortedSet<Double> lowerBound = ringUBRangeSet.headSet(distFromCentreId);
            if (lowerBound.size() == ringUBRangeSet.size()) {
                // add to result 
                UUID potentialRingId = findCorrectspondBoundedRing(allBoundedRingWithin, lowerBound.last());
                result.add(new ImmutablePair<Integer,UUID>(centreId, potentialRingId));
                // add to priority queue 
                lowerBound.remove(lowerBound.last());
                if (lowerBound.size() > 0) {
                    UUID tempRingId = findCorrectspondBoundedRing(allBoundedRingWithin, lowerBound.last());
                    pq.add(new ImmutableTriple<Double,Integer,UUID>(distFromCentreId - lowerBound.last(),
                                                                    centreId, tempRingId));
                }
            } else {
                // add to result 
                SortedSet<Double> upperBound = ringUBRangeSet.tailSet(distFromCentreId);
                UUID potentialRingId = findCorrectspondBoundedRing(allBoundedRingWithin, upperBound.first());
                result.add(new ImmutablePair<Integer,UUID>(centreId, potentialRingId));

                // add to priority queue 
                if (lowerBound.size() > 0) {
                    UUID tempRingId = findCorrectspondBoundedRing(allBoundedRingWithin, lowerBound.last());
                    pq.add(new ImmutableTriple<Double,Integer,UUID>(distFromCentreId - lowerBound.last(), 
                                                                    centreId, tempRingId));
                }

                if (upperBound.size() > 1) {
                    double lbRange = upperBound.first(); 
                    upperBound.remove(lbRange);

                    UUID tempRingId = findCorrectspondBoundedRing(allBoundedRingWithin, upperBound.first());
                    pq.add(new ImmutableTriple<Double,Integer,UUID>(lbRange - distFromCentreId, 
                                                                    centreId, tempRingId));
                }
            }

        }

        while (result.size() < A) {
            ImmutableTriple<Double, Integer, UUID> closestRing = pq.poll();
            if (closestRing == null) {
                break;
            }

            result.add(new ImmutablePair<Integer,UUID>(closestRing.getMiddle(), 
                                                       closestRing.getRight()));
        }

        return result;
    }
    
    @Override
    public void execute(Tuple input) {
        Event incomeEvent = (Event) input.getValueByField(EVENT_FIELD);
        int limit = incomeEvent.getLimit();
        List<Integer> eventCoord = incomeEvent.getCoord();
        K = limit; 
        B = 2;
        A = Math.round(1.5f * limit) / B;

        List<ImmutablePair<Integer, UUID> > potentialBoundedRings = findPotentialBoundedRings(eventCoord, K, A, B);
        List<Integer> centreList = new ArrayList<Integer>();
        List<UUID> ringList = new ArrayList<UUID>();

        for (ImmutablePair<Integer, UUID> ring: potentialBoundedRings) {
            int centreId = ring.getKey(); 
            UUID ringId = ring.getValue();

            //emit to individual bolt 
            collector.emit(INDIVIDUAL_BOUNDED_RING_HANDLER_STREAM, new Values(centreId, ringId, B));
            //gather to 1 values 
            centreList.add(centreId);
            ringList.add(ringId);

        }

        collector.emit(AGGREGATE_BOUNDED_RINGS_STREAM, new Values(incomeEvent,K, centreList, ringList));

        logger.info("********* RecommendForItemContentBased **********" + incomeEvent);
        collector.ack(input);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(INDIVIDUAL_BOUNDED_RING_HANDLER_STREAM, new Fields(CENTRE_ID_FIELD, RING_ID_FIELD, KNN_FACTOR_FIELD));
        declarer.declareStream(AGGREGATE_BOUNDED_RINGS_STREAM, new Fields(EVENT_FIELD, KNN_FACTOR_FIELD, CENTRE_LIST_FIELD, RING_LIST_FIELD));
    }
}
