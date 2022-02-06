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

public class LoggerBolt extends BaseRichBolt {

    private final Logger logger = LoggerFactory.getLogger(LoggerBolt.class);
    private OutputCollector collector;
    
    @Override
    public void prepare(Map map, TopologyContext TopologyContext, OutputCollector collector) {
        this.collector = collector;
    }
    
    @Override
    public void execute(Tuple tuple) {
        logger.info("********* LOGGER BOLT **********", tuple);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("test-output-bolt"));
    }
}
