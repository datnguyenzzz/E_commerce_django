/*
    There're 2 different sinks for checkout flow 
        +) Validated : Will be stream to provider topic
        +) Error: Stream to non-valided payment datawarehose for analytic
 */

package payment_processor.stream;

import akka.Done;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import akka.kafka.javadsl.Producer;
import akka.kafka.ProducerSettings;
import akka.stream.javadsl.Sink;

import java.util.concurrent.CompletionStage;

import com.typesafe.config.Config;

public class CheckoutSinks {
    //kafka flow: ProcedureRecord -> CompletionStage
    private final Sink<ProcedureRecord<String, String>, CompletionStage<Done>> errorSink, validatedSink;
    private final static String kafkaBrokerHost = "127.0.0.1:9092";

    public CheckoutSinks(Config config) {
        ProducerSettings<String, String> producerSettings = ProducerSettings.create(
            config,
            new StringSerializer(),
            new StringSerializer()
        )
        .withBootstrapServers(KafkaBroker);

        this.errorSink = Producer.plainSink(producerSettings);
        this.validatedSink = Producer.plainSink(producerSettings);
    }

    public Sink<ProcedureRecord<String, String>, CompletionStage<Done>> getErrorSink() {
        return errorSink;
    }

    public Sink<ProcedureRecord<String, String>, CompletionStage<Done>> getValidatedSink() {
        return validatedSink;
    }
}