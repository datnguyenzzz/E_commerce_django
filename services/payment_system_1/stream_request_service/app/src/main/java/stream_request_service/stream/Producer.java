package stream_request_service.stream; 

import akka.actor.ActorSystem;

import akka.stream.ActorMaterializer;

import akka.kafka.ProducerSettings;

import com.typesafe.config.Config;
import org.apache.kafka.common.serialization.StringSerializer;

public class Producer {

    private final ProducerSettings<String, String> producerSettings;

    private final static String akkaProducer = "akka.kafka.producer";
    private final static String akkaHost = "localhost:9092";

    public Producer(ActorSystem system, ActorMaterializer materializer) {
        final Config config = system.settings().config().getConfig(akkaProducer);
        this.producerSettings = ProducerSettings.create(
            config, 
            new StringSerializer(),
            new StringSerializer()
        ).withBootstrapServers(akkaHost);
    }
}