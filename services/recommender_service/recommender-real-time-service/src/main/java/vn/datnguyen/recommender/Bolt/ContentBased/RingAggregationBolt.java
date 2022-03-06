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
    private OutputCollector collector;
    
    @Override
    public void prepare(Map<String, Object> map, TopologyContext TopologyContext, OutputCollector collector) {
        this.collector = collector;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void execute(Tuple input) {
        List<Integer> eventCoord = (List<Integer>) input.getValueByField(EVENT_COORD_FIELD);
        int K = (int) input.getValueByField(KNN_FACTOR_FIELD);
        List<Integer> centreIdList = (List<Integer>) input.getValueByField(CENTRE_LIST_FIELD);
        List<UUID> ringIdList = (List<UUID>) input.getValueByField(RING_LIST_FIELD);

        logger.info("********* RingAggregationBolt **********" + eventCoord
                    + " KNN factor = " + K
                    + " centre list = " + centreIdList
                    + " ring list = " + ringIdList);
        collector.ack(input);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sink-bolt"));
    }
}
