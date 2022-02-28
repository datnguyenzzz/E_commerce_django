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

public class DispatcherBolt extends BaseRichBolt {
    
    private final Logger logger = LoggerFactory.getLogger(LoggerBolt.class);
    private final static CustomProperties customProperties = CustomProperties.getInstance();
    //VALUE FIELDS
    private final static String CENTRE_ID_FIELD = customProperties.getProp("CENTRE_ID_FIELD");
    private final static String EVENT_FIELD = customProperties.getProp("EVENT_FIELD");
    //incoming event 
    private final static String avroAddItemEvent = customProperties.getProp("avroAddItemEvent");
    private final static String avroDeleteItemEvent = customProperties.getProp("avroDeleteItemEvent");
    private OutputCollector collector;
    
    @Override
    public void prepare(Map<String, Object> map, TopologyContext TopologyContext, OutputCollector collector) {
        this.collector = collector;
    }
    
    @Override
    public void execute(Tuple input) {
        Event incomeEvent = (Event) input.getValueByField(EVENT_FIELD);
        String eventType = incomeEvent.getEventType();
        int centreId = (int) input.getValueByField(CENTRE_ID_FIELD);

        logger.info("********* DispatcherBolt **********" + incomeEvent + " with centre ID = " + centreId 
                    + " with event coord = " + incomeEvent.getCoord() );

        if (eventType.equals(avroAddItemEvent)) {

        } 
        else if (eventType.equals(avroDeleteItemEvent)) {

        }
        collector.ack(input);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sink-bolt"));
    }
}
