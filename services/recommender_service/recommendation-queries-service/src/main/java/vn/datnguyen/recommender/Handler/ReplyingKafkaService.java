package vn.datnguyen.recommender.Handler;

import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import vn.datnguyen.recommender.AvroClasses.AvroEvent;

@Service
public class ReplyingKafkaService {

    private static Logger logger = LoggerFactory.getLogger(ReplyingKafkaService.class);

    @Value("${transactionKafka.requestForRecommendationsTopic}")
    private String requestForRecommendationsTopic;

    @Value("${transactionKafka.defaultReplyTimeout}")
    private String defaultReplyTimeout;

    @Value("${transactionKafka.fromRecommendationServiceTopic}")
    private String fromRecommendationServiceTopic;

    private ReplyingKafkaTemplate<String, AvroEvent, Object> kafkaTemplate;

    @Autowired
    public ReplyingKafkaService(ReplyingKafkaTemplate<String, AvroEvent, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public Object requestThenReply(AvroEvent event) throws Exception {
        ProducerRecord<String, AvroEvent> record = new ProducerRecord<>(requestForRecommendationsTopic, event);
        //add header 
        record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, fromRecommendationServiceTopic.getBytes()));
        RequestReplyFuture<String, AvroEvent, Object> replyFuture = kafkaTemplate.sendAndReceive(record);
        
        SendResult<String, AvroEvent> sendResult = replyFuture.getSendFuture().get(
            Integer.parseInt(defaultReplyTimeout), TimeUnit.SECONDS);

        logger.info("Sent request successfully to " + requestForRecommendationsTopic
                    + " with status " + sendResult.getRecordMetadata()
                    + " with header = " + sendResult.getProducerRecord().headers());
        
        
        ConsumerRecord<String, Object> consumerRecord = replyFuture.get(
            Integer.parseInt(defaultReplyTimeout), TimeUnit.SECONDS);

        logger.info("Receive response successfully " + consumerRecord.value());
        
        return consumerRecord.value();
    }
}
