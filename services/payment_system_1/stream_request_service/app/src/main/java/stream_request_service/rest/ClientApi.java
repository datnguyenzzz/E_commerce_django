package stream_request_service.rest;

import akka.http.javadsl.server.AllDirectives;
import akka.http.javadsl.server.Route;

import akka.http.javadsl.marshallers.jackson.Jackson;
import akka.http.javadsl.unmarshalling.Unmarshaller;

import stream_request_service.model.PaymentRequest;

public class ClientApi extends AllDirectives {

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
                       req -> complete(req.toString()))));
    }

    public ClientApi() {}
}
