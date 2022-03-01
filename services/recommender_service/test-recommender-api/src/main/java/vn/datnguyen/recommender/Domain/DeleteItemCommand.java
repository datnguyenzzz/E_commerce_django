package vn.datnguyen.recommender.Domain;

public class DeleteItemCommand extends Command {
    private String itemId, clientId;
    
    public DeleteItemCommand(String clientId, String itemId) {
        this.itemId = itemId;
        this.clientId = clientId;
    }

    public String getClientId() {
        return this.clientId;
    }

    public String getItemId() {
        return this.itemId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }


}
