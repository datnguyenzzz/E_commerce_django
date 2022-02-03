package vn.datnguyen.recommender.Controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import vn.datnguyen.recommender.AvroClasses.AvroEvent;
import vn.datnguyen.recommender.Handler.Consumer;
import vn.datnguyen.recommender.Handler.EventSourceService;

@Component
public class EventConsumer implements Consumer {

    private EventSourceService eventSourceService;

    @Autowired
    public EventConsumer(EventSourceService eventSourceService) {
        this.eventSourceService = eventSourceService;
    }

    @KafkaListener(topics = "${ConsumerKafka.topicConsumerFromRatingCommand}", id = "${ConsumerKafka.groupId}")
    @Override
    public void execute(AvroEvent event) {
        this.eventSourceService.process(event);
    }
}
