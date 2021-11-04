package stream_request_service.rest;

import akka.actor.ActorSystem;

import akka.http.javadsl.server.AllDirectives;
import akka.http.javadsl.server.Route;

import akka.stream.ActorMaterializer;

import akka.http.javadsl.marshallers.jackson.Jackson;
import akka.http.javadsl.unmarshalling.Unmarshaller;

import stream_request_service.model.PaymentRequest;
import stream_request_service.stream.Producer;

public class ClientApi extends AllDirectives {

    private Producer producer;

    public Route createRoute() {
        return concat(getApi(),postApi());
    }

    private Route getApi() {
        return get(() -> 
            path("payment_api", () ->
                complete("Get Api")));
    }

    private Route postApi() {
        //return extractRequest(req -> complete(req.method().name() + " " + req.entity()));
        return post(() ->
            path("payment_api", () -> 
                entity(Jackson.unmarshaller(PaymentRequest.class), 
                        req -> 
                            complete(req.toString())
                        )
                )
            );
    }

    public ClientApi(ActorSystem system, ActorMaterializer materializer) {
        this.producer = new Producer(system, materializer);
    }
}
