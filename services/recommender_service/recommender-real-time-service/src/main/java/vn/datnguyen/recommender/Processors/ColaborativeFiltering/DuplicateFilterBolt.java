package vn.datnguyen.recommender.Processors.ColaborativeFiltering;

import java.util.Map;

import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.JedisCommands;
import vn.datnguyen.recommender.Processors.LoggerBolt;
import vn.datnguyen.recommender.utils.CustomProperties;


public class DuplicateFilterBolt extends AbstractRedisBolt {

    private final Logger logger = LoggerFactory.getLogger(LoggerBolt.class);
    private OutputCollector collector;

    private final static CustomProperties customProperties = CustomProperties.getInstance();
    //FIELDS
    private final static String EVENT_ID_FIELD = customProperties.getProp("EVENT_ID_FIELD");
    private final static String TIMESTAMP_FIELD = customProperties.getProp("TIMESTAMP_FIELD");
    private final static String EVENT_TYPE_FIELD = customProperties.getProp("EVENT_TYPE_FIELD");
    private final static String CLIENT_ID_FIELD = customProperties.getProp("CLIENT_ID_FIELD");
    private final static String ITEM_ID_FIELD = customProperties.getProp("ITEM_ID_FIELD");
    private final static String WEIGHT_FIELD = customProperties.getProp("WEIGHT_FIELD");

    private final static String REDIS_SET_EVENT = customProperties.getProp("REDIS_SET_EVENT");

    public DuplicateFilterBolt(JedisPoolConfig config) {
        super(config);
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext TopologyContext, OutputCollector collector) {
        this.collector = collector;
    }
    
    @Override
    public void process(Tuple input) {
        logger.info("********* DUPLICATE BOLT **********" + input);
        JedisCommands jedisCommands = null;
        try {
            jedisCommands = getInstance();
            String eventId = input.getStringByField(EVENT_ID_FIELD);
            boolean isExist = jedisCommands.sismember(REDIS_SET_EVENT, eventId);
            if (isExist) {
                logger.info("DUPLICATED EVENT !!!");
            } else {
                logger.info("********* DUPLICATE BOLT **********" + ": IS OKAY!!!!!");

                jedisCommands.sadd(REDIS_SET_EVENT, eventId);

                Values values = new Values(
                    input.getStringByField(EVENT_ID_FIELD), 
                    input.getStringByField(TIMESTAMP_FIELD), 
                    input.getStringByField(EVENT_TYPE_FIELD), 
                    input.getStringByField(CLIENT_ID_FIELD), 
                    input.getStringByField(ITEM_ID_FIELD), 
                    input.getStringByField(WEIGHT_FIELD));

                collector.emit(values);
            }
        } 
        catch (Exception e) {
            this.collector.reportError(e);
            this.collector.fail(input);
        }  
        finally {
            if (jedisCommands != null) {
                returnInstance(jedisCommands);
            }
            collector.ack(input);
        }
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(EVENT_ID_FIELD, TIMESTAMP_FIELD, EVENT_TYPE_FIELD, CLIENT_ID_FIELD, ITEM_ID_FIELD, WEIGHT_FIELD));
    }
}
