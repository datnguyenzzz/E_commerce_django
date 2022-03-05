package vn.datnguyen.recommender.Bolt.ContentBased;

import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import vn.datnguyen.recommender.Models.Event;
import vn.datnguyen.recommender.utils.CustomProperties;

public class RecommendForItemContentBased extends BaseRichBolt {
    
    private final Logger logger = LoggerFactory.getLogger(RecommendForItemContentBased.class);
    private final static CustomProperties customProperties = CustomProperties.getInstance();
    //VALUE FIELDS
    private final static String EVENT_FIELD = customProperties.getProp("EVENT_FIELD");
    //
    private OutputCollector collector;
    private int K, A, B; 
    
    @Override
    public void prepare(Map<String, Object> map, TopologyContext TopologyContext, OutputCollector collector) {
        this.collector = collector;
    }
    
    @Override
    public void execute(Tuple input) {
        Event incomeEvent = (Event) input.getValueByField(EVENT_FIELD);
        int limit = incomeEvent.getLimit();
        List<Integer> eventCoord = incomeEvent.getCoord();
        K = limit; 
        B = 2;
        A = Math.round(1.5f * limit) / B;

        logger.info("********* RecommendForItemContentBased **********" + incomeEvent);
        collector.ack(input);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sink-bolt"));
    }
}
