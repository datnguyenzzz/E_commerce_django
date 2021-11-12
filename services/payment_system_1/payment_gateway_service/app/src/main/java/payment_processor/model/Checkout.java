package payment_processor.model;

import java.lang.Cloneable;
import java.util.UUID;

public final class Checkout implements Cloneable {
    private int id; 
    private int amount; 
    private UUID uuid;

    public Checkout() {}

    public Checkout(int id,int amount) {
        this.uuid = UUID.randomUUID();
        this.id = id;
        this.amount = amount;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        Checkout cloned = (Checkout)super.clone();
        return cloned;
    }

    public String getUUID() {
        return this.uuid.toString();
    }

    public int getId() {
        return this.id;
    }

    public int getAmount() {
        return this.amount;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String toString() {
        return "uuid = " + this.uuid.toString() + '\n' 
              +"id = " + this.id + '\n'
              +"Amount = " + this.amount;
    }
}