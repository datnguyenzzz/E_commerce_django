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
    private final Sink<ProducerRecord<String, String>, CompletionStage<Done>> errorSink, thirdPartySink, internalSink;
    private final static String kafkaBrokerHost = "127.0.0.1:9092";

    public CheckoutSinks(Config config) {
        ProducerSettings<String, String> producerSettings = ProducerSettings.create(
            config,
            new StringSerializer(),
            new StringSerializer()
        )
        .withBootstrapServers(kafkaBrokerHost);

        this.errorSink = Producer.plainSink(producerSettings);
        this.thirdPartySink = Producer.plainSink(producerSettings);
        this.internalSink = Producer.plainSink(producerSettings);
    }

    public Sink<ProducerRecord<String, String>, CompletionStage<Done>> getErrorSink() {
        return errorSink;
    }

    public Sink<ProducerRecord<String, String>, CompletionStage<Done>> getThirdPartySink() {
        return thirdPartySink;
    }

    public Sink<ProducerRecord<String, String>, CompletionStage<Done>> getinternalSink() {
        return internalSink;
    }
}