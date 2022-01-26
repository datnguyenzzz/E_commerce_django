package vn.datnguyen.recommender.Controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import vn.datnguyen.recommender.AvroClasses.AvroEvent;
import vn.datnguyen.recommender.Handlers.CommandServicesImpl;
import vn.datnguyen.recommender.MessageQueue.Consumer;

@Component
public class EventConsumer implements Consumer {

    private CommandServicesImpl commandServicesImpl;

    @Autowired
    public EventConsumer(CommandServicesImpl commandServicesImpl) {
        this.commandServicesImpl = commandServicesImpl;
    }
    
    @KafkaListener(topics = "${ConsumerKafka.topicConsumerFromEventSource}", id = "${ConsumerKafka.groupId}")
    @Override
    public void execute(AvroEvent event) {
        commandServicesImpl.process(event);
    }
}
