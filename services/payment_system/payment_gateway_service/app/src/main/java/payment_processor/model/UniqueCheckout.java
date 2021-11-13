package payment_processor.model;

import java.util.UUID;

public class UniqueCheckout extends Checkout {
    private UUID uuid;

    public UniqueCheckout(int id, int amount) {
        super(id,amount);
        this.uuid = UUID.randomUUID(); 
    }

    public String getUUID() {
        return this.uuid.toString();
    }

    @Override
    public String toString() {
        return "uuid = " + this.uuid.toString() + '\n'
              + super.toString();
    }
}