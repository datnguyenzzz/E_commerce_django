package vn.datnguyen.recommender.Configurations;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;

import vn.datnguyen.recommender.AvroClasses.AvroEvent;
import vn.datnguyen.recommender.Serialization.AvroEventSerializer;

@Configuration
public class RequestReplyKafkaConfigurations {

    @Value("${transactionKafka.fromRecommendationServiceTopic}")
    private String fromRecommendationServiceTopic;

    @Value("${transactionKafka.groupConsumer}")
    private String groupConsumer;

    @Value("${transactionKafka.bootstrapServers}")
    private String bootstrapServer;

    //producer
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

    @Bean
    public ReplyingKafkaTemplate<String, AvroEvent, Object> replyingKafkaTemplate(
        ProducerFactory<String, AvroEvent> producerFactory,
        ConcurrentMessageListenerContainer<String, Object> repliesContainer
    ) {
        ReplyingKafkaTemplate<String, AvroEvent, Object> replyTemplate =
            new ReplyingKafkaTemplate<>(producerFactory, repliesContainer);

        return replyTemplate;
    }

    //consumer
    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, groupConsumer);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        return new DefaultKafkaConsumerFactory<>(configs);
    }

    @Bean
    public ConcurrentMessageListenerContainer<String, Object> replyContainer(ConsumerFactory<String, Object> cf) {
        ContainerProperties containerProperties = new ContainerProperties(fromRecommendationServiceTopic);
        return new ConcurrentMessageListenerContainer<>(cf, containerProperties);
    }

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, Object> > kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = 
            new ConcurrentKafkaListenerContainerFactory<>();
        
        factory.setConsumerFactory(consumerFactory());
        return factory;
    } 
    
}
