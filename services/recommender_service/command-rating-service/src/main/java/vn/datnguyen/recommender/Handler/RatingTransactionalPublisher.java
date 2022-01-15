package vn.datnguyen.recommender.Handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import vn.datnguyen.recommender.Converter.ConvertToEventSource;
import vn.datnguyen.recommender.Domain.Event;
import vn.datnguyen.recommender.MessageQueue.Publisher;
import vn.datnguyen.recommender.Serialization.AvroEvent;

@Component
public class RatingTransactionalPublisher implements Publisher {

    private final Logger logger = LoggerFactory.getLogger(RatingTransactionalPublisher.class);

    @Value("${transactionKafka.topic}")
    private String topicName;

    private KafkaTemplate<String, AvroEvent> kafkaTemplate;
    private ConvertToEventSource converter;

    @Autowired
    public RatingTransactionalPublisher(KafkaTemplate<String, AvroEvent> kafkaTemplate, ConvertToEventSource converter) {
        this.kafkaTemplate = kafkaTemplate;
        this.converter = converter;
    }

    public RatingTransactionalPublisher() {}

    @Override
    public void execute(Event event) {
        logger.info("Attempt publishing raw event: " + event.toString());

        kafkaTemplate.executeInTransaction(op -> {
            op.send(topicName, Integer.toString(event.getPartitionId()), converter.from(event)).addCallback(this::onSuccess, this::onFailure);
            return true;
        });
    }

    private void onSuccess(final SendResult<String, AvroEvent> res) {
        logger.info("COMMAND-RATING-SERVICE: Sucessfully publish event = " 
                    + res.getProducerRecord().toString());
    }

    private void onFailure(final Throwable t) {
        logger.warn("COMMAND-RATING-SERVICE: Unable publish event " + t.getMessage());
    }
    
}
