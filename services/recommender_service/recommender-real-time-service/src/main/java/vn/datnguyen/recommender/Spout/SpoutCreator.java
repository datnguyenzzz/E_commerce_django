package vn.datnguyen.recommender.Spout;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.kafka.spout.ByTopicRecordTranslator;
import org.apache.storm.kafka.spout.FirstPollOffsetStrategy;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.kafka.spout.KafkaSpoutRetryService;
import org.apache.storm.kafka.spout.KafkaSpoutConfig.ProcessingGuarantee;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff.TimeInterval;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import vn.datnguyen.recommender.utils.CustomProperties;

public class SpoutCreator {

    private final static CustomProperties customProperties = CustomProperties.getInstance();

    private final static String BOOTSTRAP_SERVER = customProperties.getProp("BOOTSTRAP_SERVER");
    private final static String LISTEN_FROM_TOPIC = customProperties.getProp("LISTEN_FROM_TOPIC");
    private final static String CONSUMER_GROUP = customProperties.getProp("CONSUMER_GROUP");
    private final static String EVENTSOURCE_STREAM = customProperties.getProp("EVENTSOURCE_STREAM");

    private final static String TOPIC_FIELD = customProperties.getProp("TOPIC_FIELD");
    private final static String VALUE_FIELD = customProperties.getProp("VALUE_FIELD");

    public SpoutCreator() {}

    public KafkaSpout<?,?> kafkaAvroEventSpout() {
        return new KafkaSpout<>(kafkaAvroEventSpoutConfig());
    }
    
    private KafkaSpoutConfig<String, String> kafkaAvroEventSpoutConfig() {
        ByTopicRecordTranslator<String, String> byTopicTranslator = new ByTopicRecordTranslator<>(
            (r) -> new Values(r.topic(), r.value()), 
            new Fields(TOPIC_FIELD, VALUE_FIELD),
            EVENTSOURCE_STREAM);
        
        //KafkaSpoutConfig.Builder<String, String> kafkaBuilder = new KafkaSpoutConfig.Builder<String, String>(BOOTSTRAP_SERVER, new String[]{LISTEN_FROM_TOPIC});

        return KafkaSpoutConfig.builder(BOOTSTRAP_SERVER, new String[]{LISTEN_FROM_TOPIC})
            .setProp(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP)
            .setProp(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
            .setProcessingGuarantee(ProcessingGuarantee.AT_LEAST_ONCE)
            .setFirstPollOffsetStrategy(FirstPollOffsetStrategy.EARLIEST)
            .setRetry(kafkaSpoutRetryService())
            .setEmitNullTuples(false)
            .setRecordTranslator(byTopicTranslator)
            .build();
    }

    private KafkaSpoutRetryService kafkaSpoutRetryService() {
        return new KafkaSpoutRetryExponentialBackoff(TimeInterval.microSeconds(500), 
            TimeInterval.milliSeconds(2), 
            Integer.MAX_VALUE, 
            TimeInterval.seconds(20));
    }
}
