package vn.datnguyen.recommender.Bolt;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import vn.datnguyen.recommender.AvroClasses.AvroEvent;
import vn.datnguyen.recommender.utils.AvroEventScheme;
import vn.datnguyen.recommender.utils.CustomProperties;

public class WeightApplierBolt extends BaseRichBolt {
    
    public static Charset charset = Charset.forName("UTF-8");
    public static CharsetEncoder encoder = charset.newEncoder();
    public static CharsetDecoder decoder = charset.newDecoder();

    private final static CustomProperties customProperties = CustomProperties.getInstance();
    //VALUE FIELDS
    private final static String VALUE_FIELD = customProperties.getProp("VALUE_FIELD");
    private final static String EVENT_TYPE_FIELD = customProperties.getProp("EVENT_TYPE_FIELD");
    private final static String CLIENT_ID_FIELD = customProperties.getProp("CLIENT_ID_FIELD");
    private final static String ITEM_ID_FIELD = customProperties.getProp("ITEM_ID_FIELD");
    private final static String WEIGHT_FIELD = customProperties.getProp("WEIGHT_FIELD");

    private final Logger logger = LoggerFactory.getLogger(LoggerBolt.class);
    private OutputCollector collector;
    private AvroEventScheme avroEventScheme = new AvroEventScheme();
    
    @Override
    public void prepare(Map map, TopologyContext TopologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    public static ByteBuffer str_to_bb(String msg){
        try{
          return encoder.encode(CharBuffer.wrap(msg));
        }catch(Exception e){e.printStackTrace();}
        return null;
      }
    
    @Override
    public void execute(Tuple tuple) {
        String avroEventStr = (String) tuple.getValueByField(VALUE_FIELD);
        AvroEvent event = (AvroEvent) avroEventScheme.deserialize(str_to_bb(avroEventStr)).get(0);
        logger.info("********* APPLY WEIGHT BOLT **********" + event);
        collector.ack(tuple);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(EVENT_TYPE_FIELD, CLIENT_ID_FIELD, ITEM_ID_FIELD, WEIGHT_FIELD));
    }
}
