package vn.datnguyen.recommender.Bolt.ContentBased;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import vn.datnguyen.recommender.Models.Event;
import vn.datnguyen.recommender.utils.CustomProperties;

public class RecommendForItemContentBased extends BaseRichBolt {
    
    private final Logger logger = LoggerFactory.getLogger(RecommendForItemContentBased.class);
    private final static CustomProperties customProperties = CustomProperties.getInstance();
    //STREAM
    private final static String INDIVIDUAL_BOUNDED_RING_HANDLER_STREAM = customProperties.getProp("INDIVIDUAL_BOUNDED_RING_HANDLER_STREAM");
    private final static String AGGREGATE_BOUNDED_RINGS_STREAM = customProperties.getProp("AGGREGATE_BOUNDED_RINGS_STREAM");
    //FIELDs
    private final static String CENTRE_ID_FIELD = customProperties.getProp("CENTRE_ID_FIELD");
    private final static String RING_ID_FIELD = customProperties.getProp("RING_ID_FIELD");
    private final static String KNN_FACTOR_FIELD = customProperties.getProp("KNN_FACTOR_FIELD");
    private final static String RINGS_LIST_SIZE_FIELD = customProperties.getProp("RINGS_LIST_SIZE_FIELD");
    private final static String RING_LIST_FIELD = customProperties.getProp("RING_LIST_FIELD");
    private final static String CENTRE_LIST_FIELD = customProperties.getProp("CENTRE_LIST_FIELD");
    //VALUE FIELDS
    private final static String EVENT_FIELD = customProperties.getProp("EVENT_FIELD");
    //
    private OutputCollector collector;
    private int K, A, B; 
    
    @Override
    public void prepare(Map<String, Object> map, TopologyContext TopologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    private double dist(List<Integer> a, List<Integer> b) {
        double s = 0;
        for (int i=0; i<a.size(); i++) {
            s += (a.get(i) - b.get(i)) * (a.get(i) - b.get(i));
        }
        return Math.sqrt(s);
    }

    private List<ImmutablePair<Integer, UUID> > findPotentialBoundedRings(List<Integer> eventCoord, int K, int A, int B) {
        return null;
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

        collector.emit(AGGREGATE_BOUNDED_RINGS_STREAM, new Values(K, potentialBoundedRings.size(), centreList, ringList));

        logger.info("********* RecommendForItemContentBased **********" + incomeEvent);
        collector.ack(input);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(INDIVIDUAL_BOUNDED_RING_HANDLER_STREAM, new Fields(CENTRE_ID_FIELD, RING_ID_FIELD, KNN_FACTOR_FIELD));
        declarer.declareStream(AGGREGATE_BOUNDED_RINGS_STREAM, new Fields(KNN_FACTOR_FIELD, RINGS_LIST_SIZE_FIELD, CENTRE_LIST_FIELD, RING_LIST_FIELD));
    }
}
