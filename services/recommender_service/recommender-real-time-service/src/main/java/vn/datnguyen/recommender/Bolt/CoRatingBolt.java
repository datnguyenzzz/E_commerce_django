package vn.datnguyen.recommender.Bolt;

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

public class CoRatingBolt extends BaseRichBolt {
    
    private final Logger logger = LoggerFactory.getLogger(CoRatingBolt.class);
    private final static CustomProperties customProperties = CustomProperties.getInstance();
    //VALUE FIELDS
    private final static String OLD_RATING = customProperties.getProp("OLD_RATING");
    private final static String EVENT_FIELD = customProperties.getProp("EVENT_FIELD");
    private OutputCollector collector;
    
    @Override
    public void prepare(Map<String, Object> map, TopologyContext TopologyContext, OutputCollector collector) {
        this.collector = collector;
    }
    
    @Override
    public void execute(Tuple input) {
        Event incomeEvent = (Event) input.getValueByField(EVENT_FIELD);
        int oldRating = (int) input.getValueByField(OLD_RATING);

        logger.info("********* CoRatingBolt **********" + incomeEvent + " with old rating = " + oldRating);
        collector.ack(input);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sink-bolt"));
    }
}