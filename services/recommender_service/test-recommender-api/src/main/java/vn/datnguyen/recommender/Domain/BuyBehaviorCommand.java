package vn.datnguyen.recommender.Domain;

import java.util.Locale;

public class BuyBehaviorCommand extends Command {
    private String clientId; 
    private String itemId; 

    public BuyBehaviorCommand() {}

    public BuyBehaviorCommand(String clientId, String itemId) {
        this.clientId = clientId; 
        this.itemId = itemId; 
    }

    public String getClientId() {
        return this.clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getItemId() {
        return this.itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    @Override
    public String toString() {
        return String.format(Locale.getDefault(), 
                            "BuyBehaviorCommand(clientId=%s,itemId=%s)",
                            getClientId(), getItemId());
    }
}
