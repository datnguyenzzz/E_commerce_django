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

public class KnnBolt extends BaseRichBolt {
    
    private final Logger logger = LoggerFactory.getLogger(KnnBolt.class);
    private final static CustomProperties customProperties = CustomProperties.getInstance();
    //field tuple
    private final static String EVENT_COORD_FIELD = customProperties.getProp("EVENT_COORD_FIELD");
    private final static String CENTRE_ID_FIELD = customProperties.getProp("CENTRE_ID_FIELD");
    private final static String RING_ID_FIELD = customProperties.getProp("RING_ID_FIELD");
    private final static String KNN_FACTOR_FIELD = customProperties.getProp("KNN_FACTOR_FIELD");
    //
    private OutputCollector collector;
    
    @Override
    public void prepare(Map<String, Object> map, TopologyContext TopologyContext, OutputCollector collector) {
        this.collector = collector;
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
        collector.ack(input);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sink-bolt"));
    }
}
