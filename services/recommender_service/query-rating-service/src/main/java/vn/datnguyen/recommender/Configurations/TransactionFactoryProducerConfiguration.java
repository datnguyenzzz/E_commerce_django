package vn.datnguyen.recommender.Configurations;

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

import vn.datnguyen.recommender.AvroClasses.AvroEvent;
import vn.datnguyen.recommender.Serialization.AvroEventSerializer;

@Configuration
public class TransactionFactoryProducerConfiguration {

    @Value("${ConsumerKafka.bootstrapServers}")
    private String bootstrapServer;

    @Value("${ConsumerKafka.transactionPrefix}")
    private String transactionIdPrefix;

    @Bean
    public ProducerFactory<String, AvroEvent> producerFactory() {
        Map<String, Object> config = new HashMap<>();

        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroEventSerializer.class);

        DefaultKafkaProducerFactory<String, AvroEvent> factory = new DefaultKafkaProducerFactory<>(config);
        factory.setTransactionIdPrefix(transactionIdPrefix);

        return factory;
    }

    @Bean
    public KafkaTemplate<String, AvroEvent> kafkaTemplate(@Autowired ProducerFactory<String, AvroEvent> factory) {
        return new KafkaTemplate<>(factory);
    }
}
