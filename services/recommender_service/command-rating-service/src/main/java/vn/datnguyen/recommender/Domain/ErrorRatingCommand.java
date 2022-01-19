package vn.datnguyen.recommender.Domain;

import java.util.Locale;

public class ErrorRatingCommand extends Command {
    private String clientId; 
    private String itemId;
    private String error; 

    public ErrorRatingCommand(String error) {
        this(null, null, error);
    }

    public ErrorRatingCommand(String clientId, String itemId, String error) {
        this.clientId = clientId; 
        this.itemId = itemId; 
        this.error = error;
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

    public String getError() {
        return this.error;
    }

    public void setError(String error) {
        this.error = error;
    }

    @Override
    public String toString() {
        return String.format(Locale.getDefault(), 
                            "ErrorRatingCommand(clientId=%s,itemId=%s,error=%s)",
                            getClientId(), getItemId(),getError());
    }
}
