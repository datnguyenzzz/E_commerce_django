package vn.datnguyen.recommender.Configurations;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;

import vn.datnguyen.recommender.AvroClasses.AvroEvent;
import vn.datnguyen.recommender.Serialization.AvroEventSerializer;

@Configuration
public class RequestReplyKafkaConfigurations {

    @Value("${transactionKafka.defaultReplyTimeout}")
    private String defaultReplyTimeout;

    @Value("${transactionKafka.fromRecommendationServiceTopic}")
    private String fromRecommendationServiceTopic;

    @Value("${transactionKafka.groupConsumer}")
    private String groupConsumer;

    @Value("${transactionKafka.bootstrapServers}")
    private String bootstrapServer;

    @Bean
    public ReplyingKafkaTemplate<String, AvroEvent, Object> replyingKafkaTemplate(
        ProducerFactory<String, AvroEvent> producerFactory,
        ConcurrentMessageListenerContainer<String, Object> repliesContainer
    ) {
        ReplyingKafkaTemplate<String, AvroEvent, Object> replyTemplate =
            new ReplyingKafkaTemplate<>(producerFactory, repliesContainer);

        replyTemplate.setDefaultReplyTimeout(Duration.ofSeconds(Integer.parseInt(defaultReplyTimeout)));
        replyTemplate.setSharedReplyTopic(true);
        return replyTemplate;
    }

    @Bean
    public ConcurrentMessageListenerContainer<String, Object> repliesContainer(
        ConcurrentKafkaListenerContainerFactory<String, Object> containerFactory
    ) {
        ConcurrentMessageListenerContainer<String, Object> repliesContainer = 
            containerFactory.createContainer(fromRecommendationServiceTopic);
        repliesContainer.getContainerProperties().setGroupId(groupConsumer);
        repliesContainer.setAutoStartup(false);
        return repliesContainer;
    }

    @Bean
    public ProducerFactory<String, AvroEvent> producerFactory() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroEventSerializer.class);

        DefaultKafkaProducerFactory<String, AvroEvent> producerFactory = 
            new DefaultKafkaProducerFactory<>(configs);

        return producerFactory;
    }
}
