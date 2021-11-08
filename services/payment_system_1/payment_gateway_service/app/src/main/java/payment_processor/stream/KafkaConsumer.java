package payment_processor.stream;

import akka.Done;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;

import akka.kafka.Subscriptions;
import akka.kafka.AutoSubscription;
import akka.kafka.ConsumerSettings;
import akka.kafka.javadsl.Consumer;
import akka.stream.javadsl.Sink;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.concurrent.CompletionStage;

import com.typesafe.config.Config;

public class KafkaConsumer {
    private final static String kafkaConsumerConfig = "akka.kafka.consumer";
    private final static String kafkaProducerConfig = "akka.kafka.producer";
    private final static String kafkaBrokerHost = "127.0.0.1:9092";
    private final static String groupId = "payment-gateway-1";
    private final static String topicName = "checkouts";
    private final static int parallelism = 4;

    private ConsumerSettings<String, String> consumerSettings;
    private final ActorSystem system;
    private final ActorMaterializer materializer;
    private AutoSubscription subscription;

    private CheckoutSinks checkoutSinks;

    public String getName() {
        return "Payment gateway start !!!";
    }

    public CompletionStage<Done> consume() {
        return Consumer.plainSource(this.consumerSettings, this.subscription)
            .map(ConsumerRecord::value) //json
            .runForeach(
                event -> System.out.println(event.toString()), this.materializer
            );
    }

    public KafkaConsumer(ActorSystem system, ActorMaterializer materializer) {
        this.system = system; 
        this.materializer = materializer;
        //-------------------------------------------//
        Config consumerConfig = system.settings().config().getConfig(kafkaConsumerConfig);
        Config producerConfig = system.settings().config().getConfig(kafkaProducerConfig);
        this.consumerSettings = ConsumerSettings.create(
            consumerConfig, 
            new StringDeserializer(),
            new StringDeserializer()
        )
        .withBootstrapServers(kafkaBrokerHost)
        .withGroupId(groupId)
        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //-------------------------------------------//
        this.subscription = Subscriptions.topics(topicName);
        //-------------------------------------------//
        this.checkoutSinks = new CheckoutSinks(producerConfig);

    }
}