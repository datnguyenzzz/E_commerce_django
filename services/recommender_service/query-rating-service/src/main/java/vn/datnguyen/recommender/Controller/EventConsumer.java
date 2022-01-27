package vn.datnguyen.recommender.Controller;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import javax.transaction.Transactional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import vn.datnguyen.recommender.AvroClasses.AvroDeleteRating;
import vn.datnguyen.recommender.AvroClasses.AvroEvent;
import vn.datnguyen.recommender.AvroClasses.AvroPublishRating;
import vn.datnguyen.recommender.AvroClasses.AvroUpdateRating;
import vn.datnguyen.recommender.Handler.EventHandler;
import vn.datnguyen.recommender.MessageQueue.Consumer;
import vn.datnguyen.recommender.Repositories.RatingRepository;

@Service
public class EventConsumer implements Consumer, EventHandler {

    private final Logger logger = LoggerFactory.getLogger(EventConsumer.class);

    private RatingRepository ratingRepository;

    @Autowired
    public EventConsumer(RatingRepository ratingRepository) {
        this.ratingRepository = ratingRepository;
    }

    @Override
    public CompletableFuture<Void> process(AvroEvent event) {
        return CompletableFuture.runAsync(
            () -> Stream.of(event)
                .forEach(this::apply)
        );
    }

    @Override
    public void apply(AvroEvent event) {
        logger.info("QUERY-RATING-SERVICE: consumed event = " + event);

        Object eventPayload = event.getData(); 

        if (eventPayload instanceof AvroPublishRating) {
            apply((AvroPublishRating) eventPayload);
        }
        else if (eventPayload instanceof AvroUpdateRating) {
            apply((AvroUpdateRating) eventPayload);
        }
        else if (eventPayload instanceof AvroDeleteRating)  {
            apply((AvroDeleteRating) eventPayload);
        }
    }

    @Transactional
    private void apply(AvroPublishRating payload) {
        String clientId = payload.getClientId(); 
        String itemId = payload.getItemId(); 
        int score = payload.getScore();
    }

    @Transactional
    private void apply(AvroUpdateRating payload) {
        String clientId = payload.getClientId(); 
        String itemId = payload.getItemId(); 
        int score = payload.getScore();
    } 

    @Transactional
    private void apply(AvroDeleteRating payload) {
        String clientId = payload.getClientId(); 
        String itemId = payload.getItemId(); 

    }

    @KafkaListener(topics = "${ConsumerKafka.topicConsumerFromEventSource}", id = "${ConsumerKafka.groupId}")
    @Override
    public void execute(AvroEvent event) {
        process(event);
    }
}
