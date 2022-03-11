package vn.datnguyen.recommender.Bolt.ContentBased;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import vn.datnguyen.recommender.utils.CustomProperties;

public class RingAggregationBolt extends BaseRichBolt {
    
    private final Logger logger = LoggerFactory.getLogger(RingAggregationBolt.class);
    private final static CustomProperties customProperties = CustomProperties.getInstance();
    //VALUE FIELDS
    private final static String EVENT_COORD_FIELD = customProperties.getProp("EVENT_COORD_FIELD");
    private final static String KNN_FACTOR_FIELD = customProperties.getProp("KNN_FACTOR_FIELD");
    private final static String RING_LIST_FIELD = customProperties.getProp("RING_LIST_FIELD");
    private final static String CENTRE_LIST_FIELD = customProperties.getProp("CENTRE_LIST_FIELD");
    private final static String ITEM_ID_LIST_FIELD = customProperties.getProp("ITEM_ID_LIST_FIELD");
    private final static String DIST_LIST_FIELD = customProperties.getProp("DIST_LIST_FIELD");
    private final static String CENTRE_ID_FIELD = customProperties.getProp("CENTRE_ID_FIELD");
    private final static String RING_ID_FIELD = customProperties.getProp("RING_ID_FIELD");
    private final static String EVENT_ID_FIELD = customProperties.getProp("EVENT_ID_FIELD");
    //stream 
    private final static String AGGREGATE_BOUNDED_RINGS_STREAM = customProperties.getProp("AGGREGATE_BOUNDED_RINGS_STREAM");
    private final static String INDIVIDUAL_KNN_ALGORITHM_STREAM = customProperties.getProp("INDIVIDUAL_KNN_ALGORITHM_STREAM");
    //
    private OutputCollector collector;
    //

    
    @Override
    public void prepare(Map<String, Object> map, TopologyContext TopologyContext, OutputCollector collector) {
        this.collector = collector;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void execute(Tuple input) {

        String tupleSource = input.getSourceStreamId();

        if (tupleSource.equals(AGGREGATE_BOUNDED_RINGS_STREAM)) {

            List<Integer> eventCoord = (List<Integer>) input.getValueByField(EVENT_COORD_FIELD);
            String eventId = (String) input.getValueByField(EVENT_ID_FIELD);
            int K = (int) input.getValueByField(KNN_FACTOR_FIELD);
            List<Integer> centreIdList = (List<Integer>) input.getValueByField(CENTRE_LIST_FIELD);
            List<String> ringIdList = (List<String>) input.getValueByField(RING_LIST_FIELD);

            logger.info("********* RingAggregationBolt **********: FROM AGGREGATE_BOUNDED_RINGS_STREAM" 
                        + " eventId = " + eventId
                        + " eventCoord = " + eventCoord
                        + " KNN factor = " + K
                        + " centre list = " + centreIdList
                        + " ring list = " + ringIdList);
        
        } 
        else if (tupleSource.equals(INDIVIDUAL_KNN_ALGORITHM_STREAM)) {
            List<Integer> eventCoord = (List<Integer>) input.getValueByField(EVENT_COORD_FIELD);
            int centreId = (int) input.getValueByField(CENTRE_ID_FIELD);
            UUID ringId = UUID.fromString((String) input.getValueByField(RING_ID_FIELD));
            List<String> itemIdList = (List<String>) input.getValueByField(ITEM_ID_LIST_FIELD);
            List<Double> distList = (List<Double>) input.getValueByField(DIST_LIST_FIELD);

            logger.info("********* RingAggregationBolt **********: FROM INDIVIDUAL_KNN_ALGORITHM_STREAM" 
                        + eventCoord
                        + " centreId = " + centreId
                        + " ringId = " + ringId 
                        + " itemIdList = " + itemIdList
                        + " distList = " + distList);
        }
        collector.ack(input);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sink-bolt"));
    }
}
