package vn.datnguyen.recommender.Bolt.ContentBased;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
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
    private final static String EVENT_COORD_FIELD = customProperties.getProp("EVENT_COORD_FIELD");
    private final static String CENTRE_ID_FIELD = customProperties.getProp("CENTRE_ID_FIELD");
    private final static String RING_ID_FIELD = customProperties.getProp("RING_ID_FIELD");
    private final static String KNN_FACTOR_FIELD = customProperties.getProp("KNN_FACTOR_FIELD");
    private final static String RING_LIST_FIELD = customProperties.getProp("RING_LIST_FIELD");
    private final static String CENTRE_LIST_FIELD = customProperties.getProp("CENTRE_LIST_FIELD");
    private final static String EVENT_ID_FIELD = customProperties.getProp("EVENT_ID_FIELD");
    //WEIGHT VALUEs
    private final static String KEYSPACE_FIELD = customProperties.getProp("KEYSPACE_FIELD");
    private final static String NUM_NODE_REPLICAS_FIELD = customProperties.getProp("NUM_NODE_REPLICAS_FIELD");
    //CASSANDRA PROPS
    private final static String CASS_NODE = customProperties.getProp("CASS_NODE");
    private final static String CASS_PORT = customProperties.getProp("CASS_PORT");
    private final static String CASS_DATA_CENTER = customProperties.getProp("CASS_DATA_CENTER");
    //knn factor 
    private final static String MIN_FACTOR_EXPECTED_EACH_RING = customProperties.getProp("MIN_FACTOR_EXPECTED_EACH_RING");
    //table col
    //-- centre
    private final static String CENTRE_ID = "centre_id";
    private final static String CENTRE_COORD = "centre_coord";
    private final static String CENTRE_UPPER_BOUND_RANGE_LIST = "centre_upper_bound_range_list";
    //--- bounded ring
    private final static String RING_ID = "ring_id"; // UUID
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

    private long dist(List<Integer> a, List<Integer> b) {
        long s = 0;
        for (int i=0; i<a.size(); i++) {
            s += (a.get(i) - b.get(i)) * (a.get(i) - b.get(i));
        }
        return s;
    }

    private UUID findCorrectspondBoundedRing(int centreId, long expectedUBRange) {
        SimpleStatement findRingByIdAndRangeStatement = 
            this.boundedRingRepository.findBoundedRingByIdAndRange(centreId, expectedUBRange);
        
        ResultSet findRingByIdAndRangeRes = 
            this.repositoryFactory.executeStatement(findRingByIdAndRangeStatement, KEYSPACE_FIELD); 
        
        if (findRingByIdAndRangeRes.getAvailableWithoutFetching() == 0) {
            logger.warn("********* RecommendForItemContentBased **********: Must return 1, found 0 in centre - " 
                        + centreId + " with range - " + expectedUBRange);
            return null;          
        }

        Row findRingByIdAndRange = findRingByIdAndRangeRes.one();
        UUID ringId = (UUID) this.repositoryFactory.getFromRow(findRingByIdAndRange, RING_ID);
        return ringId;
    }

    private List<ImmutablePair<Integer, UUID> > findPotentialBoundedRings(List<Integer> eventCoord, int K, int A, int B) {
        
        List<ImmutablePair<Integer, UUID> > result = new ArrayList<>();
        Map<Integer, List<Long> > UBRangeOfCentre = new HashMap<>();
        Map<Integer, List<Integer> > CoordOfCentre = new HashMap<>();

        Comparator<ImmutableTriple<Integer, Integer, Boolean> > customCompare = (tuple1, tuple2) -> {
            int centreId1 = tuple1.getLeft();
            int ubRangePos1 = tuple1.getMiddle();
            long ubRange1 = UBRangeOfCentre.get(centreId1).get(ubRangePos1);
            long dist1 = dist(eventCoord, CoordOfCentre.get(centreId1));

            int centreId2 = tuple2.getLeft();
            int ubRangePos2 = tuple2.getMiddle();
            long ubRange2 = UBRangeOfCentre.get(centreId2).get(ubRangePos2);
            long dist2 = dist(eventCoord, CoordOfCentre.get(centreId2));

            long diff1 = Math.abs(ubRange1 - dist1);
            long diff2 = Math.abs(ubRange2 - dist2);

            int cmp = (diff1 < diff2) ? 1 : -1;
            return cmp;
        };

        //CentreId position direction
        PriorityQueue<ImmutableTriple<Integer, Integer, Boolean> > pq = new PriorityQueue<>(customCompare);

        //get all centre and rings within
        SimpleStatement allCentreStatement = this.indexesCoordRepository.selectAllCentre();
        List<Row> allCentre = 
            this.repositoryFactory.executeStatement(allCentreStatement, KEYSPACE_FIELD)
                .all();

        for (Row r: allCentre) {
            int centreId = (int) this.repositoryFactory.getFromRow(r, CENTRE_ID);
            List<Integer> centreCoord = this.repositoryFactory.getListIntegerFromRow(r, CENTRE_COORD);
            List<Long> ringUBRangeList = this.repositoryFactory.getListLongFromRow(r, CENTRE_UPPER_BOUND_RANGE_LIST);

            UBRangeOfCentre.put(centreId, ringUBRangeList);
            CoordOfCentre.put(centreId, centreCoord);
        }

        for (Row r: allCentre) {
            //centre 
            int centreId = (int) this.repositoryFactory.getFromRow(r, CENTRE_ID); 
            List<Integer> centreCoord = this.repositoryFactory.getListIntegerFromRow(r, CENTRE_COORD);
            List<Long> ringUBRangeList = this.repositoryFactory.getListLongFromRow(r, CENTRE_UPPER_BOUND_RANGE_LIST);
            SortedSet<Long> ringUBRangeSet = new TreeSet<>(ringUBRangeList);

            if (ringUBRangeSet.size() == 0) {
                continue;
            }

            long distFromCentreId = dist(centreCoord, eventCoord);

            // find all lower bound 
            SortedSet<Long> lowerBound = ringUBRangeSet.headSet(distFromCentreId);

            if (lowerBound.size() == ringUBRangeSet.size()) {
                logger.info("********* RecommendForItemContentBased **********: current point > MAX ub range");
                boolean isDown = true; 
                int pos = lowerBound.size() - 1;
                logger.info("********* RecommendForItemContentBased **********: Add to heap: "
                            + "centreId = " + centreId
                            + "pos = " + pos
                            + "isDown = " +  isDown);
                pq.add(new ImmutableTriple<Integer,Integer,Boolean>(centreId, pos, isDown));
            } else {
                logger.info("********* RecommendForItemContentBased **********: current point inside max ubrange");
                int lbPos = lowerBound.size() - 1;

                //ring contain point
                UUID potentialRingId = findCorrectspondBoundedRing(centreId, ringUBRangeList.get(lbPos+1));
                logger.info("********* RecommendForItemContentBased **********: Add to result: "
                            + "centreId = " + centreId
                            + " ringId = " + potentialRingId
                            + " upper range = " + ringUBRangeList.get(lbPos+1)
                            + " curr dist = " + distFromCentreId);
                result.add(new ImmutablePair<Integer,UUID>(centreId, potentialRingId));

                //add 2 adjacency ring to pq
                boolean isDown = true;
                if (lbPos >= 0) {
                    logger.info("********* RecommendForItemContentBased **********: Add to heap: "
                            + "centreId = " + centreId
                            + " pos = " + lbPos
                            + " isDown = " +  isDown);
                    pq.add(new ImmutableTriple<Integer,Integer,Boolean>(centreId, lbPos, isDown));
                }

                isDown = false; 
                lbPos += 2; 
                if (lbPos < ringUBRangeSet.size()) {
                    logger.info("********* RecommendForItemContentBased **********: Add to heap: "
                            + "centreId = " + centreId
                            + " pos = " + lbPos
                            + " isDown = " +  isDown);
                    pq.add(new ImmutableTriple<Integer,Integer,Boolean>(centreId, lbPos, isDown));
                }
            }

        }

        while (result.size() < A && pq.size()>0) {
            ImmutableTriple<Integer,Integer,Boolean> closestRing = pq.poll();
            if (closestRing == null) {
                break;
            }

            int centreId = closestRing.getLeft();
            int ubRangePos = closestRing.getMiddle();
            boolean isDown = closestRing.getRight();
            long ubRange = UBRangeOfCentre.get(centreId).get(ubRangePos);
            UUID ringId = findCorrectspondBoundedRing(centreId, ubRange);

            logger.info("********* RecommendForItemContentBased **********: Add to result: "
                            + "centreId = " + centreId
                            + " ringId = " + ringId
                            + " upper range = " + ubRange
                            + " distance diff = " + Math.abs(ubRange - dist(CoordOfCentre.get(centreId), eventCoord)));
            result.add(new ImmutablePair<Integer,UUID>(centreId, ringId));

            ubRangePos += (isDown) ? -1 : 1;
            if (ubRangePos >=0 && ubRangePos < UBRangeOfCentre.get(centreId).size()) {
                logger.info("********* RecommendForItemContentBased **********: Add to heap: "
                            + "centreId = " + centreId
                            + " pos = " + ubRangePos
                            + " isDown = " +  isDown);
                pq.add(new ImmutableTriple<Integer,Integer,Boolean>(centreId, ubRangePos, isDown));
            }
        }

        return result;
    }
    
    @Override
    public void execute(Tuple input) {
        Event incomeEvent = (Event) input.getValueByField(EVENT_FIELD);
        String eventId = incomeEvent.getEventId();
        int limit = incomeEvent.getLimit();
        List<Integer> eventCoord = incomeEvent.getCoord();
        K = limit; 
        B = Integer.parseInt(MIN_FACTOR_EXPECTED_EACH_RING);
        A = Math.round(1.5f * limit) / B;

        logger.info("********* RecommendForItemContentBased **********"
                    + "event coord = " + eventCoord
                    + " K = " + K
                    + " B = " + B
                    + " A = " + A);

        List<ImmutablePair<Integer, UUID> > potentialBoundedRings = findPotentialBoundedRings(eventCoord, K, A, B);
        List<Integer> centreList = new ArrayList<Integer>();
        List<String> ringList = new ArrayList<String>();

        for (ImmutablePair<Integer, UUID> ring: potentialBoundedRings) {
            int centreId = ring.getKey(); 
            UUID ringId = ring.getValue();

            logger.info("********* RecommendForItemContentBased **********: Sent signal to "
                    + " centreId = " + centreId
                    + " ringId = " + ringId);

            //emit to individual bolt 
            collector.emit(INDIVIDUAL_BOUNDED_RING_HANDLER_STREAM, new Values(eventId, eventCoord, centreId, ringId.toString(), B));
            //gather to 1 values 
            centreList.add(centreId);
            ringList.add(ringId.toString());

        }

        collector.emit(AGGREGATE_BOUNDED_RINGS_STREAM, new Values(eventId, eventCoord, K, centreList, ringList));

        collector.ack(input);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(INDIVIDUAL_BOUNDED_RING_HANDLER_STREAM, new Fields(EVENT_ID_FIELD, EVENT_COORD_FIELD, CENTRE_ID_FIELD, RING_ID_FIELD, KNN_FACTOR_FIELD));
        declarer.declareStream(AGGREGATE_BOUNDED_RINGS_STREAM, new Fields(EVENT_ID_FIELD, EVENT_COORD_FIELD, KNN_FACTOR_FIELD, CENTRE_LIST_FIELD, RING_LIST_FIELD));
    }
}
