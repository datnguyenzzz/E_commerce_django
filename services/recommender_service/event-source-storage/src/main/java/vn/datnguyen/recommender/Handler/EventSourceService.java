package vn.datnguyen.recommender.Handler;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import vn.datnguyen.recommender.AvroClasses.AvroEvent;
import vn.datnguyen.recommender.Cache.Repository.CachedEvent;
import vn.datnguyen.recommender.Cache.Repository.CachedEventRepository;

@Service
public class EventSourceService implements EventHandler {

    private Logger logger = LoggerFactory.getLogger(EventConsumer.class);

    private CachedEventRepository cachedEventRepository;

    @Autowired
    public EventSourceService(CachedEventRepository cachedEventRepository) {
        this.cachedEventRepository = cachedEventRepository;
    }
    
    @Override
    public CompletableFuture<Void> process(AvroEvent event) {
        //logger.info("EVENT-SOURCE-STORAGE: consumer event " + event);
        return CompletableFuture.runAsync(
            () -> Stream.of(event).filter( ev -> {
                    try {
                        return isEventDuplicated(ev);
                    }
                    catch (Exception e) {
                        logger.warn("EVENT-SOURCE-STORAGE: filter exception" + e);
                    }
                    return false;
                }
            )
            .map(this::cachingEvent)
            .forEach(this::testLogging)
        );
    }

    private boolean isEventDuplicated(AvroEvent event) {
        String eventId = event.getEventId(); 
        boolean isEventCached = cachedEventRepository.existsById(eventId);
        return (!isEventCached);
    }

    private AvroEvent cachingEvent(AvroEvent event) {
        String eventId = event.getEventId(); 
        String eventType = event.getEventType();
        CachedEvent cacheEvent = new CachedEvent(eventId, eventType);
        cachedEventRepository.save(cacheEvent);
        return event;
    }
    
    private void testLogging(AvroEvent event) {
        logger.info("EVENT-SOURCE-STORAGE: consumer event after caching" + event);
    }

    @Override
    public void apply(AvroEvent event) {}

}
