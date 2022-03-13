package vn.datnguyen.recommender.Handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import vn.datnguyen.recommender.AvroClasses.AvroEvent;

@Component
public class RatingTransactionalPublisher implements Publisher {

    private final Logger logger = LoggerFactory.getLogger(RatingTransactionalPublisher.class);

    @Value("${transactionKafka.topic}")
    private String topicName;

    @Value("${transactionKafka.requestForRecommendationsTopic}")
    private String requestForRecommendationsTopic;

    @Value("${incomingEvent.avroRecommendForItemEvent}")
    private String avroRecommendForItemEvent;

    private KafkaTemplate<String, AvroEvent> kafkaTemplate;

    @Autowired
    public RatingTransactionalPublisher(KafkaTemplate<String, AvroEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public RatingTransactionalPublisher() {}

    @Override
    public void execute(AvroEvent event) {
        logger.info("Attempt publishing raw event: " + event.toString() + " to topic" + topicName + "-" + Integer.toString(event.getPartitionId()));

        String eventType = event.getEventType();
        if (eventType.equals(avroRecommendForItemEvent)) {
            kafkaTemplate.executeInTransaction(op -> {
                op.send(requestForRecommendationsTopic, event).addCallback(this::onSuccess, this::onFailure);
                return true;
            });
        }
        else {
            kafkaTemplate.executeInTransaction(op -> {
                op.send(topicName, Integer.toString(event.getPartitionId()), event)
                    .addCallback(this::onSuccess, this::onFailure);
                return true;
            });
        }
    }

    private void onSuccess(final SendResult<String, AvroEvent> res) {
        logger.info("COMMAND-RATING-SERVICE: Sucessfully publish event = " 
                    + res.getProducerRecord().toString()
                    + "onto topic: "
                    + topicName);
    }

    private void onFailure(final Throwable t) {
        logger.warn("COMMAND-RATING-SERVICE: Unable publish event " + t.getMessage());
    }
    
}
