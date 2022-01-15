package vn.datnguyen.recommender.Handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import vn.datnguyen.recommender.Domain.Event;
import vn.datnguyen.recommender.MessageQueue.Publisher;

@Component
public class RatingTransactionalPublisher implements Publisher {

    private final Logger logger = LoggerFactory.getLogger(RatingTransactionalPublisher.class);

    public RatingTransactionalPublisher() {}

    @Override
    public void execute(Event event) {
        logger.info("Attemp publish event: " + event.toString());
    }
    
}