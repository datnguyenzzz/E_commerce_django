package payment_processor.stream;

import akka.Done;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;

import akka.kafka.Subscriptions;
import akka.kafka.AutoSubscription;
import akka.kafka.ConsumerSettings;
import akka.kafka.javadsl.Consumer;
import akka.stream.javadsl.Sink;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.concurrent.CompletionStage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;

import io.vavr.control.Either; 
import io.vavr.Tuple2;

import payment_processor.model.Checkout;

public class KafkaConsumer {
    private final static String kafkaConsumerConfig = "akka.kafka.consumer";
    private final static String kafkaProducerConfig = "akka.kafka.producer";
    private final static String kafkaBrokerHost = "127.0.0.1:9092";
    private final static String groupId = "payment-gateway-1"; //each consumer has same group_id consume same topic
    private final static String topicName = "checkouts";
    private final static String errorTopic = "error-payment"; 
    private final static String thirdPartyPSPTopic = "third-party-psp";
    private final static String internalPSPTopic = "posession-psp";
    private final static int parallelism = 4;

    private ConsumerSettings<String, String> consumerSettings;
    private final ActorSystem system;
    private final ActorMaterializer materializer;
    private AutoSubscription subscription;
    private final ObjectMapper objectMapper;

    private CheckoutSinks checkoutSinks;

    public String getName() {
        return "Payment gateway start !!!";
    }

    public CompletionStage<Done> consume() {
        return Consumer.plainSource(this.consumerSettings, this.subscription)
            .map(this::demarshallingMessage) 
            .divertTo(
                checkoutSinks.getErrorSink().contramap(this::toErrorTopic), //map function before to sink
                Either::isLeft
            ) // divert to sink if Either.isLeft()
            .map(Either::get) // Get right or throw left
            .runForeach(
                event -> System.out.println(event.toString()), this.materializer
            );
    }

    //marshall to class for database store purpose
    private Either<String, Checkout> demarshallingMessage(ConsumerRecord<String,String> message) {
        try {
            return Either.right(objectMapper.readValue(message.value(), Checkout.class));
        } catch (JsonProcessingException ex) {
            return Either.left("Error while demarshall message key = " + message.key());
        }
    }

    //Fraud detection

    //check user for distributing to correctsponded topic

    //error demarshalled message
    private ProducerRecord<String,String> toErrorTopic(Either<String, Checkout> ex) {
        return new ProducerRecord<>(errorTopic, ex.getLeft());
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
        //-------------------------------------------//
        this.objectMapper = new ObjectMapper();

    }
}