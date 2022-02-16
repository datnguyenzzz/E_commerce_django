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

import vn.datnguyen.recommender.utils.CustomProperties;

public class SimilaritiesBolt extends BaseRichBolt {
    
    private final Logger logger = LoggerFactory.getLogger(SimilaritiesBolt.class);
    private final static CustomProperties customProperties = CustomProperties.getInstance();
    private static final String ITEM_COUNT_BOLT = customProperties.getProp("ITEM_COUNT_BOLT");
    private static final String PAIR_COUNT_BOLT = customProperties.getProp("PAIR_COUNT_BOLT");
    //Item count
    private final static String ITEM_ID_FIELD = customProperties.getProp("ITEM_ID_FIELD");
    private final static String OLD_ITEM_COUNT = customProperties.getProp("OLD_ITEM_COUNT");
    private final static String NEW_ITEM_COUNT = customProperties.getProp("NEW_ITEM_COUNT");
    //pair count 
    private final static String ITEM_1_ID_FIELD = customProperties.getProp("ITEM_1_ID_FIELD");
    private final static String ITEM_2_ID_FIELD = customProperties.getProp("ITEM_2_ID_FIELD");
    private final static String OLD_PAIR_COUNT = customProperties.getProp("OLD_PAIR_COUNT");
    private final static String NEW_PAIR_COUNT = customProperties.getProp("NEW_PAIR_COUNT");
    //VALUE FIELDS
    private OutputCollector collector;
    
    @Override
    public void prepare(Map<String, Object> map, TopologyContext TopologyContext, OutputCollector collector) {
        this.collector = collector;
    }
    
    @Override
    public void execute(Tuple input) {
        String inputSource = input.getSourceComponent();
        logger.info("************ SimilaritiesBolt *************: Tuple input from " + inputSource);

        if (inputSource.equals(ITEM_COUNT_BOLT)) {
            String itemId = (String) input.getValueByField(ITEM_ID_FIELD);
            int oldItemCount = (int) input.getValueByField(OLD_ITEM_COUNT);
            int newItemCount = (int) input.getValueByField(NEW_ITEM_COUNT);
            logger.info("************ SimilaritiesBolt *************: Values - " + itemId + " . " + oldItemCount + " . " + newItemCount);
        } else if (inputSource.equals(PAIR_COUNT_BOLT)) {
            String item1Id = (String) input.getValueByField(ITEM_1_ID_FIELD);
            String item2Id = (String) input.getValueByField(ITEM_2_ID_FIELD);
            int oldPairCount = (int) input.getValueByField(OLD_PAIR_COUNT);
            int newPairCount = (int) input.getValueByField(NEW_PAIR_COUNT);
            logger.info("************ SimilaritiesBolt *************: Values - " + item1Id + " . " + item2Id + " . " + oldPairCount + " . " + newPairCount);
        }
        collector.ack(input);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sink-bolt"));
    }
}
