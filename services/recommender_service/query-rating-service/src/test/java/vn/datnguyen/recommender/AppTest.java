package vn.datnguyen.recommender;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.web.WebAppConfiguration;

import junit.framework.Assert;
import vn.datnguyen.recommender.Models.Rating;
import vn.datnguyen.recommender.Repositories.RatingRepository;

/**
 * Unit test for simple App.
 */
@SpringBootTest(classes = App.class)
@WebAppConfiguration
public class AppTest {

    @Autowired
    private RatingRepository ratingRepository;

    private final String neverExistClientId = "xxx-xxx-xxx";
    private final String neverExistItemId = "xxx-xxx-xxx";
    private final int score = 12343;

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

    @Test
    public void addNewRecord() {
        List<Rating> result = (List<Rating>)ratingRepository.findAll();

        Assert.assertEquals(result.size(), 0);

        Rating rating = new Rating(neverExistClientId, neverExistItemId, score);

        ratingRepository.save(rating);
        result = (List<Rating>)ratingRepository.findAll();

        Assert.assertEquals(result.size(), 1);
    }

    @Test
    public void findRatingByClientAndItemIdAndDelete() {
        String wrongClientId = "xxx";
        String wrongItemId = "xxx";
        List<Rating> result;

        result = ratingRepository.findByClientIdAndItemId(wrongClientId, neverExistItemId);
        Assert.assertEquals("WRONG CLIENT ID", result.size(), 0);

        result = ratingRepository.findByClientIdAndItemId(neverExistClientId, wrongItemId);
        Assert.assertEquals("WRONG ITEM ID", result.size(), 0);

        result = ratingRepository.findByClientIdAndItemId(wrongClientId, wrongItemId);
        Assert.assertEquals("WRONG CLIENT ID AND ITEM ID",result.size(), 0);

        result = ratingRepository.findByClientIdAndItemId(neverExistClientId, neverExistItemId);
        Assert.assertEquals("PROPER CLIENT ID AND ITEM ID", result.size(), 1);

        Rating rating = result.get(0);
        Assert.assertEquals(rating.getClientId(), neverExistClientId);
        Assert.assertEquals(rating.getItemId(), neverExistItemId);
        Assert.assertEquals(rating.getScore(), score);
        ratingRepository.delete(rating);

        result = (List<Rating>)ratingRepository.findAll();

        Assert.assertEquals(result.size(), 0);
    }
    
}
