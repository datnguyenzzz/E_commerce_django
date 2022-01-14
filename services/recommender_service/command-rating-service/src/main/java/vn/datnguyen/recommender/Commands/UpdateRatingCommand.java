package vn.datnguyen.recommender.Commands;

import java.util.Locale;

public class UpdateRatingCommand extends Command {
    private String clientId; 
    private String itemId; 
    private int score; 

    public UpdateRatingCommand(String clientId, String itemId, int score) {
        this.clientId = clientId; 
        this.itemId = itemId; 
        this.score = score;
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

    public int getScore() {
        return this.score;
    }

    public void setScore(int score) {
        this.score = score;
    }

    @Override
    public String toString() {
        return String.format(Locale.getDefault(), 
                            "UpdateRatingCommand(clientId=%s,itemId=%s,score=%d)",
                            getClientId(), getItemId(), getScore());
    }
}
