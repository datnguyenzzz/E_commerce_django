package vn.datnguyen.recommender.MessageQueue;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import vn.datnguyen.recommender.Serialization.AvroEvent;

@Configuration
public class TransactionFactoryProducerConfiguration {

    @Value("${transactionKafka.transactionIdPrefix}")
    private String transactionIdPrefix;

    @Value("${transactionKafka.bootstrapServers}")
    private String bootstrapServer;

    @Bean
    public ProducerFactory<String, AvroEvent> producerFactory() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"");

        DefaultKafkaProducerFactory<String, AvroEvent> factory = new DefaultKafkaProducerFactory<>(configs);
        factory.setTransactionIdPrefix(transactionIdPrefix);

        return factory;
    }

    public KafkaTemplate<String, AvroEvent> kafkaTemplate(@Autowired ProducerFactory<String, AvroEvent> factory) {
        return new KafkaTemplate<>(factory);
    }
}
