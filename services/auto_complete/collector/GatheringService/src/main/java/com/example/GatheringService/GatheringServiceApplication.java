package com.example.GatheringService;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;

@SpringBootApplication
public class GatheringServiceApplication {

	@Value("${topic.name}") 
	private String topicName; 

	@Value("${topic.partitions-num}")
	private int partitions; 

	@Value ("${topic.replication-factor}")
	private int replicationFactor;

	@Bean
	public NewTopic WordFromClientTopic() {
		return TopicBuilder.name(topicName)
				.partitions(partitions)
				.replicas(replicationFactor)
				.compact()
				.build();
	}

	public static void main(String[] args) {
		SpringApplication.run(GatheringServiceApplication.class, args);
	}

}
