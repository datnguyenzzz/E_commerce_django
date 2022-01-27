package vn.datnguyen.recommender.Controller;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import vn.datnguyen.recommender.AvroClasses.AvroEvent;
import vn.datnguyen.recommender.Handler.EventHandler;
import vn.datnguyen.recommender.MessageQueue.Consumer;

@Service
public class EventConsumer implements Consumer, EventHandler {

    private final Logger logger = LoggerFactory.getLogger(EventConsumer.class);

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
    }
    
    @KafkaListener(topics = "${ConsumerKafka.topicConsumerFromEventSource}", id = "${ConsumerKafka.groupId}")
    @Override
    public void execute(AvroEvent event) {
        process(event);
    }
}
