package stream_request_service.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public final class PaymentRequest {
    private int id; 
    private int amount; 

    @JsonCreator
    public PaymentRequest(@JsonProperty("id") int id,@JsonProperty("amount") int amount) {
        this.id = id;
        this.amount = amount;
    }

    public int getId() {
        return this.id;
    }

    public int getAmount() {
        return this.amount;
    }

    public String toString() {
        return "id = " + this.id + '\n'
              +"Amount = " + this.amount;
    }
}