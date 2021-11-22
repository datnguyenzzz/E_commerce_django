package com.example.GatheringService.producer;

import com.example.GatheringService.dto.GatherRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class Producer {

    @Value("${topic.name}")
    private String topicName;

    private final KafkaTemplate<String, GatherRequest> kafkaTemplate;

    @Autowired
    public Producer(KafkaTemplate<String, GatherRequest> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendEvent(GatherRequest gatherRequest) {
        this.kafkaTemplate.send(this.topicName, gatherRequest);
    }
}
