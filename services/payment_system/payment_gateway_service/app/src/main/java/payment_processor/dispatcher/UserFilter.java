package payment_processor.dispatcher; 

import payment_processor.model.UniqueCheckout;

public class UserFilter {
    private UniqueCheckout checkout; 

    private final static String errorTopic = "error-payment"; 
    private final static String thirdPartyPSPTopic = "third-party-psp";
    private final static String internalPSPTopic = "internal-psp";

    public UserFilter(UniqueCheckout checkout) {
        try {
            this.checkout = (UniqueCheckout) checkout.clone();
        } catch (CloneNotSupportedException ex) {}
    }

    public String clientRoute() {
        //specific function filter each client to suitable topic 
        int clientId = this.checkout.getId();

        if (clientId > 1000) {
            return errorTopic;
        }
        
        if (clientId % 2 == 0) {
            return thirdPartyPSPTopic; 
        }
        
        if (clientId % 2 == 1) {
            return internalPSPTopic;
        }

        return errorTopic;       
    }
}