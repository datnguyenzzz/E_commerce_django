package payment_processor.model;

import java.util.UUID;
import java.time.format.DateTimeFormatter;  
import java.time.LocalDateTime;   

public class UniqueCheckout extends Checkout {
    private UUID uuid;
    private LocalDateTime now;

    public UniqueCheckout(int id, int amount) {
        super(id,amount);
        this.uuid = UUID.randomUUID(); 
        this.now = LocalDateTime.now();  
    }

    public String getUUID() {
        return this.uuid.toString();
    }

    public String getTimeNow() {
        DateTimeFormatter formater = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");  
        return formater.format(this.now).toString();
    }

    @Override
    public String toString() {
        return "uuid = " + this.uuid.toString() + '\n'
              + this.getTimeNow() + '\n'
              + super.toString();
    }
}