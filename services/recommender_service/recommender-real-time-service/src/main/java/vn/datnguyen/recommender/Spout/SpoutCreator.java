package vn.datnguyen.recommender.Spout;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.kafka.spout.FirstPollOffsetStrategy;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.kafka.spout.KafkaSpoutRetryService;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff.TimeInterval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import vn.datnguyen.recommender.Serialization.AvroEventDeserializer;
import vn.datnguyen.recommender.utils.CustomProperties;

public class SpoutCreator {

    private final Logger logger = LoggerFactory.getLogger(SpoutCreator.class);
    private final static CustomProperties customProperties = new CustomProperties();

    private final static String bootstrapServer = customProperties.getProp("bootstrapServers");
    private final static String listenFromTopic = customProperties.getProp("listenFromTopic");

    public SpoutCreator() {}

    public KafkaSpout<?,?> kafkaSpout() {
        logger.info("RECOMMENDER-SERVICE: " + "kakfa spout from host = " + bootstrapServer 
            + "with topic = " + listenFromTopic);

        return new KafkaSpout<>(kafkaSpoutConfig());
    }

    private KafkaSpoutConfig<String, String> kafkaSpoutConfig() {
        return KafkaSpoutConfig.builder(bootstrapServer, listenFromTopic)
            .setProp(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
            .setProp(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AvroEventDeserializer.class)
            .setFirstPollOffsetStrategy(FirstPollOffsetStrategy.EARLIEST)
            .setRetry(kafkaSpoutRetryService())
            .build();
    }

    private KafkaSpoutRetryService kafkaSpoutRetryService() {
        return new KafkaSpoutRetryExponentialBackoff(TimeInterval.microSeconds(500), 
            TimeInterval.milliSeconds(2), 
            Integer.MAX_VALUE, 
            TimeInterval.seconds(20));
    }
}
