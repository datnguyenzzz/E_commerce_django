package payment_processor.fraud;

import payment_processor.model.Checkout;

public class Classification {
    private Checkout checkout; 

    public Classification(Checkout checkout) {
        System.out.println(checkout.toString());
        try {
            this.checkout = (Checkout) checkout.clone();
        } catch (CloneNotSupportedException ex) {}
    }

    public boolean result() {
        //True - fraud
        //False - otherwise
        return (this.checkout.getAmount() > 10000000);
    }
}