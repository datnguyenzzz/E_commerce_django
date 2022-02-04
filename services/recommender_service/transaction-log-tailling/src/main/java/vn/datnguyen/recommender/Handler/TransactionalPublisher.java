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
public class TransactionalPublisher implements Publisher {

    @Value("${transactionKafka.topicToQueryService}")
    private String topicToQueryService;

    @Value("${transactionKafka.topicToRecommendSerivce}")
    private String topicToRecommendSerivce;

    @Value("${incomingEvent.avroPublishRatingEvent}")
    private String avroPublishRatingEvent;

    @Value("${incomingEvent.avroUpdateRatingEvent}")
    private String avroUpdateRatingEvent;

    @Value("${incomingEvent.avroDeleteRatingEvent}")
    private String avroDeleteRatingEvent;

    private final Logger logger = LoggerFactory.getLogger(TransactionalPublisher.class);
    private KafkaTemplate<String, AvroEvent> kafkaTemplate;
    
    @Autowired
    public TransactionalPublisher(KafkaTemplate<String, AvroEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    private boolean isCommandRatingEvent(AvroEvent event) {
        return ((event.getEventType().equals(avroPublishRatingEvent)) || 
                (event.getEventType().equals(avroUpdateRatingEvent)) || 
                (event.getEventType().equals(avroDeleteRatingEvent)));
    }

    @Override
    public void execute(AvroEvent event) {
        logger.info("EVENT-SOURCE: Transaction publish event " + event);

        kafkaTemplate.executeInTransaction(op -> {

            if (isCommandRatingEvent(event)) {
                op.send(topicToQueryService, Integer.toString(event.getPartitionId()), event)
                    .addCallback(this::onSuccessToQueryService, this::onFailureToQueryService);
            }
            // send to recommendation service
            // op.send();
            return true;
        });
    }

    private void onSuccessToQueryService(final SendResult<String, AvroEvent> res) {
        logger.info("EVENT-SOURCE: Sucessfully publish event to QUERY-RATING-SERVICE = " 
                    + res.getProducerRecord().toString());
    }

    private void onFailureToQueryService(final Throwable t) {
        logger.warn("EVENT-SOURCE: Unable publish event to QUERY-RATING-SERVICE =" + t.getMessage());
    }
}
