package payment_processor.model;

public class Checkout implements Cloneable {
    private int id; 
    private int amount; 

    public Checkout() {}

    public Checkout(int id,int amount) {
        this.id = id;
        this.amount = amount;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
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
        return "id = " + this.id + '\n'
              +"Amount = " + this.amount;
    }
}