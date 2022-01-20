package vn.datnguyen.recommender.Handler;

import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import vn.datnguyen.recommender.AvroClasses.AvroEvent;

@Service
public class EventSourceService implements EventHandler {

    private Logger logger = LoggerFactory.getLogger(EventConsumer.class);

    public EventSourceService() {}
    
    @Override
    public CompletableFuture<Void> process(AvroEvent event) {
        logger.info("EVENT-SOURCE-STORAGE: consumer event " + event);
        return null;
    }

    @Override
    public void apply(AvroEvent event) {

    }
}
