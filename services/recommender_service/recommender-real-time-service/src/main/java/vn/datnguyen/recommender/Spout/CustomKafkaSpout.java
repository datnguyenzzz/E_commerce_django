package vn.datnguyen.recommender.Spout;

import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomKafkaSpout<K,V> extends KafkaSpout<K,V> {

    private final static Logger logger = LoggerFactory.getLogger(CustomKafkaSpout.class);

    public CustomKafkaSpout(KafkaSpoutConfig<K,V> config) {
        super(config);
    }

    @Override
    public void fail(Object messageId) {
        logger.warn("*********KAFKA SPOUT ********: ACKING FAILED - " + messageId);
        super.fail(messageId);
    }

    @Override
    public void ack(Object messageId) {
        logger.info("*********KAFKA SPOUT ********: ACKING SUCCESSFULLY - " + messageId);
        super.ack(messageId);
    }
}
