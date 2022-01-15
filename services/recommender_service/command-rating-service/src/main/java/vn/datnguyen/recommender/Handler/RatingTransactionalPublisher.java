package vn.datnguyen.recommender.Handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import vn.datnguyen.recommender.Converter.ConvertToEventSource;
import vn.datnguyen.recommender.Domain.Event;
import vn.datnguyen.recommender.MessageQueue.Publisher;

@Component
public class RatingTransactionalPublisher implements Publisher {

    private final Logger logger = LoggerFactory.getLogger(RatingTransactionalPublisher.class);

    private ConvertToEventSource converter;

    @Autowired
    public RatingTransactionalPublisher(ConvertToEventSource converter) {
        this.converter = converter;
    }

    public RatingTransactionalPublisher() {}

    @Override
    public void execute(Event event) {
        logger.info("Attemp publishing raw event: " + event.toString());
        logger.info("Attemp publishing event source: " + converter.from(event));
    }
    
}
