package payment_processor.fraud;

import payment_processor.model.UniqueCheckout;

public class Classification {
    private UniqueCheckout checkout; 

    public Classification(UniqueCheckout checkout) {
        try {
            this.checkout = (UniqueCheckout) checkout.clone();
        } catch (CloneNotSupportedException ex) {}
    }

    public boolean result() {
        //True - fraud
        //False - otherwise
        return (this.checkout.getAmount() > 10000000);
    }
}