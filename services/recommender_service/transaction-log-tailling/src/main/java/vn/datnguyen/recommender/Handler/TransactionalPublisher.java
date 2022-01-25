package vn.datnguyen.recommender.Handler;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import vn.datnguyen.recommender.AvroClasses.AvroEvent;
import vn.datnguyen.recommender.MessageQueue.Publisher;

@Component
public class TransactionalPublisher implements Publisher {

    private final Logger logger = LoggerFactory.getLogger(TransactionalPublisher.class);
    
    public TransactionalPublisher() {}

    @Override
    public void execute(AvroEvent event) {
        logger.info("EVENT-SOURCE: Transaction publish event " + event);
    }
}
