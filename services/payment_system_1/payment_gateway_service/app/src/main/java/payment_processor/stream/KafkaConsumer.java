package payment_processor.stream;

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.kafka.ConsumerSettings;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import com.typesafe.config.Config;

public class KafkaConsumer {
    private final static String kafkaConsumerConfig = "akka.kafka.consumer";
    private final static String kafkaBrokerHost = "127.0.0.1:9092";
    private final static String groupId = "payment-gateway-1";

    private ConsumerSettings<String, String> consumerSettings;
    private final ActorSystem system;
    private final ActorMaterializer materializer;

    public String getName() {
        return "Wiii";
    }

    public KafkaConsumer(ActorSystem system, ActorMaterializer materializer) {
        this.system = system; 
        this.materializer = materializer;

        final Config config = system.settings().config().getConfig(kafkaConsumerConfig);
        this.consumerSettings = ConsumerSettings.create(
            config, 
            new StringDeserializer(),
            new StringDeserializer()
        )
        .withBootstrapServers(kafkaBrokerHost)
        .withGroupId(groupId)
        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }
}