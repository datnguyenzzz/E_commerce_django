package vn.datnguyen.recommender.Domain;

public class RecommendationForItemCommand extends Command {
    private String itemId;
    private int limit, property1, property2, property3;
    
    public RecommendationForItemCommand(String itemId, int limit, int property1, int property2, int property3) {
        this.itemId = itemId;
        this.limit = limit;
        this.property1 = property1;
        this.property2 = property2;
        this.property3 = property3;
    }


    public String getItemId() {
        return this.itemId;
    }

    public int getLimit() {
        return this.limit;
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

    public void setLimit(int limit) {
        this.limit = limit;
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
