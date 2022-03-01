package vn.datnguyen.recommender.Domain;

public class AddItemCommand extends Command {
    private String itemId, clientId;
    private int property1, property2, property3;
    
    public AddItemCommand(String clientId, String itemId, int property1, int property2, int property3) {
        this.itemId = itemId;
        this.clientId = clientId;
        this.property1 = property1;
        this.property2 = property2;
        this.property3 = property3;
    }

    public String getClientId() {
        return this.clientId;
    }

    public String getItemId() {
        return this.itemId;
    }

    public int getProperty1() {
        return this.property1;
    }

    public int getProperty2() {
        return this.property2;
    }

    public int getProperty3() {
        return this.property3;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    public void setProperty1(int property1) {
        this.property1 = property1;
    }

    public void setProperty2(int property2) {
        this.property2 = property2;
    }

    public void setProperty3(int property3) {
        this.property3 = property3;
    }

}
