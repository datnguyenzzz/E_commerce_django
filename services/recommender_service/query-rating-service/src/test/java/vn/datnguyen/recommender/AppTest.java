package vn.datnguyen.recommender;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import junit.framework.Assert;
import vn.datnguyen.recommender.Models.Rating;

/**
 * Unit test for simple App.
 */

@SpringBootTest(classes = App.class)
public class AppTest {
    @Test
    public void testRatingModel() {
        String clientId = "321-451-312";
        String itemId = "320-312-512";
        int score = 213;

        Rating rating = new Rating(clientId, itemId, score);

        Assert.assertEquals(rating.getClientId(), clientId);
        Assert.assertEquals(rating.getItemId(), itemId);
        Assert.assertEquals(rating.getScore(), score);

        String newClientId = "321-451-312-312";
        String newItemId = "320-312-512-123";
        int newScore = 2213;

        rating.setClientId(newClientId);
        rating.setItemId(newItemId);
        rating.setScore(newScore);

        Assert.assertEquals(rating.getClientId(), newClientId);
        Assert.assertEquals(rating.getItemId(), newItemId);
        Assert.assertEquals(rating.getScore(), newScore);
    }
    
}
