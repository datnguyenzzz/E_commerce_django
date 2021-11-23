package com.example.GatheringService.producer;

import com.example.GatheringService.Word;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

// field injection vs contructor injection 
// FI accept all value -> Some value might be unprocessable
// CI only accept when provided wired field 

@Service
public class Producer {

    @Value("${topic.name}")
    private String topicName;

    //@Autowired can't wire final like this
    private final KafkaTemplate<String, Word> kafkaTemplate;

    @Autowired
    public Producer(KafkaTemplate<String, Word> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }
    
    public void sendEvent(Word word) {
        this.kafkaTemplate.send(this.topicName,word.getContext().toString(), word);
    }
}
