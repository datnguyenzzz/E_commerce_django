package stream_request_service.stream; 

import akka.Done;

import akka.actor.ActorSystem;

import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Source;

import akka.kafka.ProducerSettings;
import akka.kafka.javadsl.Producer;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.vavr.control.Try;

import java.util.concurrent.CompletionStage;
import com.typesafe.config.Config;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import stream_request_service.model.PaymentRequest;

public class KafkaProducer {

    private ProducerSettings<String, String> producerSettings;
    private ActorMaterializer materializer;

    private final static String kafkaProducer = "akka.kafka.producer";
    private final static String kafkaBrokerHost = "localhost:9092";
    private final static String kafkaTopic = "checkouts";

    private static final ObjectMapper mapper = new ObjectMapper();

    private Try<String> toJson(PaymentRequest req) {
        // Instances of Try, are either an instance of Success or Failure
        return Try.of(() ->
            mapper.writeValueAsString(req)
        );
    }

    private ProducerRecord<String, String> toProducerRecord(String reqJson) {
        return new ProducerRecord<>(kafkaTopic, reqJson);
    }

    public CompletionStage<Done> produceToKafka(PaymentRequest req) {
        CompletionStage<Done> completion = Source.single(req) 
            .map(this::toJson)
            .filter(Try::isSuccess)
            .map(Try::get)
            .map(this::toProducerRecord)
            .runWith(Producer.plainSink(this.producerSettings), this.materializer);
        
        return completion;
    }

    public KafkaProducer(ActorSystem system, ActorMaterializer materializer) {
        final Config config = system.settings().config().getConfig(kafkaProducer);

        this.materializer = materializer;
        this.producerSettings = ProducerSettings.create(
            config, 
            new StringSerializer(),
            new StringSerializer()
        ).withBootstrapServers(kafkaBrokerHost);
    }
}