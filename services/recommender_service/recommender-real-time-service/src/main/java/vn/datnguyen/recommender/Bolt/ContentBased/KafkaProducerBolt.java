package vn.datnguyen.recommender.Bolt.ContentBased;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import vn.datnguyen.recommender.utils.CustomProperties;

public class KafkaProducerBolt<K,V> extends KafkaBolt<K,V> {

    private final static CustomProperties customProperties = CustomProperties.getInstance();

    private final static Logger logger = LoggerFactory.getLogger(KafkaProducerBolt.class);
    private final static String KAFKA_MESSAGE_HEADER_FIELD = customProperties.getProp("KAFKA_MESSAGE_HEADER_FIELD");
    private final static String KAFKA_VALUE_FIELD = customProperties.getProp("KAFKA_VALUE_FIELD");
    //default by Spring Kafka lib
    private final static String KAFKA_TOPIC_FIELD = "kafka_replyTopic";

    private Producer<K, V> producer;
    private OutputCollector collector;
    private Properties boltSpecifiedProperties = new Properties();

    public KafkaProducerBolt() {
        super();
    }

    public KafkaProducerBolt<K, V> withProducerProperties(Properties producerProperties) {
        this.boltSpecifiedProperties = producerProperties;
        return this;
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        logger.info("Preparing bolt with configuration {}", this);
        //for backward compatibility.

        this.producer = super.mkProducer(boltSpecifiedProperties);
        this.collector = collector;
    }
    
    @SuppressWarnings({"unused", "unchecked"})
    @Override
    protected void process(final Tuple input) {
        try {
            V value = (V) input.getValueByField(KAFKA_VALUE_FIELD);
            RecordHeaders headers = (RecordHeaders) input.getValueByField(KAFKA_MESSAGE_HEADER_FIELD);

            String topic = null;

            Iterator<Header> headerIterater = headers.iterator();
            while (headerIterater.hasNext()) {
                Header header = (Header) headerIterater.next();
                String headerKey = header.key();
                byte[] headerBytes = header.value();

                if (headerKey.equals(KAFKA_TOPIC_FIELD)) {
                    topic = Arrays.toString(headerBytes);
                    break;
                }
            }

            if (topic == null) {
                logger.warn("**************** KAFKA PRODUCER****************: Topic is missing in header");
            } else {
                Future<RecordMetadata> result = this.producer.send(
                    new ProducerRecord<K,V>(topic, null, null, value, headers),
                    null
                );

                collector.ack(input);
            }
        }
        catch (Exception ex) {
            collector.reportError(ex);
            collector.fail(input);
        }
    }
}
