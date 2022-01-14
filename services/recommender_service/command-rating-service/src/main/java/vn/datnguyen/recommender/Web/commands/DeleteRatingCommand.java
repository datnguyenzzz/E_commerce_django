package vn.datnguyen.recommender.Web.commands;

import java.util.Locale;

public class DeleteRatingCommand {
    private String clientId; 
    private String itemId; 

    public DeleteRatingCommand() {}

    public DeleteRatingCommand(String clientId, String itemId) {
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
                            "DeleteRatingCommand(clientId=%s,itemId=%s)",
                            getClientId(), getItemId());
    }
}
