package vn.datnguyen.recommender.Handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import vn.datnguyen.recommender.MessageQueue.Consumer;
import vn.datnguyen.recommender.Serialization.AvroEvent;

@Component
public class EventConsumer implements Consumer {

    private Logger logger = LoggerFactory.getLogger(EventConsumer.class);

    public EventConsumer() {}

    @KafkaListener(topics = "${ConsumerKafka.topicConsumerFromRatingCommand}", id = "${ConsumerKafka.groupId}")
    @Override
    public void execute(AvroEvent event) {
        logger.info("EVENT-SOURCE-STORAGE: consumer event " + event);
    }
}
