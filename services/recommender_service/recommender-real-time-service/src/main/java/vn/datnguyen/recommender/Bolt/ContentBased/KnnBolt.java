package vn.datnguyen.recommender.Bolt.ContentBased;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
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
    private int knnFactor;
    private List<Integer> eventCoord;
    //priority queue <itemId>
    private PriorityQueue<String> pq;

    private Comparator<String > customeComparator = (t1, t2) -> {
        return 0;
    };

    @Override
    public void prepare(Map<String, Object> map, TopologyContext TopologyContext, OutputCollector collector) {
        this.collector = collector;
        this.knnFactor = 0; 
        this.pq = new PriorityQueue<>(customeComparator);
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

        this.knnFactor = knnFactor;
        this.eventCoord = eventCoord;


        collector.ack(input);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sink-bolt"));
    }
}
