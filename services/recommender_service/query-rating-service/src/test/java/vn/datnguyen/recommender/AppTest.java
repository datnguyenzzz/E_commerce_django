package vn.datnguyen.recommender;
/*
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.web.WebAppConfiguration;

import junit.framework.Assert;
import vn.datnguyen.recommender.Models.Rating;
import vn.datnguyen.recommender.Repositories.RatingRepository;

@SpringBootTest(classes = App.class)
@WebAppConfiguration*/
public class AppTest {
    /*
    @Autowired
    private RatingRepository ratingRepository;
    
    private final String neverExistClientId = "xxx-xxx-xxx";
    private final String neverExistItemId = "xxx-xxx-xxx";
    private final int score = 12343;


    @Test
    public void findRatingByClientAndItemIdAndDelete() {
        Rating rating = new Rating(neverExistClientId, neverExistItemId, score);
        ratingRepository.save(rating);

        String wrongClientId = "xxx";
        String wrongItemId = "xxx";
        List<Rating> result;

        result = ratingRepository.findByClientIdAndItemId(wrongClientId, neverExistItemId);
        Assert.assertEquals("WRONG CLIENT ID", 0, result.size());

        result = ratingRepository.findByClientIdAndItemId(neverExistClientId, wrongItemId);
        Assert.assertEquals("WRONG ITEM ID", 0, result.size());

        result = ratingRepository.findByClientIdAndItemId(wrongClientId, wrongItemId);
        Assert.assertEquals("WRONG CLIENT ID AND ITEM ID", 0,result.size());

        result = ratingRepository.findByClientIdAndItemId(neverExistClientId, neverExistItemId);
        Assert.assertEquals("PROPER CLIENT ID AND ITEM ID", 1, result.size());

        rating = result.get(0);
        Assert.assertEquals(neverExistClientId, rating.getClientId());
        Assert.assertEquals(neverExistItemId, rating.getItemId());
        Assert.assertEquals(score, rating.getScore());
        ratingRepository.delete(rating);

        result = (List<Rating>)ratingRepository.findAll();

        Assert.assertEquals(0, result.size());
    }

    @Test
    public void findRatingByClientIdAndDelete() {

        Rating rating = new Rating(neverExistClientId, neverExistItemId, score);
        ratingRepository.save(rating);

        String wrongClientId = "xxx";
        List<Rating> result;

        result = ratingRepository.findByClientId(wrongClientId);
        Assert.assertEquals("WRONG CLIENT ID", 0, result.size());

        result = ratingRepository.findByClientId(neverExistClientId);
        Assert.assertEquals("PROPER CLIENT ID", 1, result.size());

        rating = result.get(0);
        Assert.assertEquals(neverExistClientId, rating.getClientId());
        Assert.assertEquals(neverExistItemId, rating.getItemId());
        Assert.assertEquals(score, rating.getScore());
        ratingRepository.delete(rating);

        result = (List<Rating>)ratingRepository.findAll();

        Assert.assertEquals(result.size(), 0);
    }

    @Test
    public void findRatingByItemIdAndDelete() {

        Rating rating = new Rating(neverExistClientId, neverExistItemId, score);
        ratingRepository.save(rating);

        String wrongItemId = "xxx";
        List<Rating> result;

        result = ratingRepository.findByItemId(wrongItemId);
        Assert.assertEquals("WRONG ITEM ID", 0, result.size());

        result = ratingRepository.findByItemId(neverExistItemId);
        Assert.assertEquals("PROPER ITEM ID", 1, result.size());

        rating = result.get(0);
        Assert.assertEquals(neverExistClientId, rating.getClientId());
        Assert.assertEquals(neverExistItemId, rating.getItemId());
        Assert.assertEquals(score, rating.getScore());
        ratingRepository.delete(rating);

        result = (List<Rating>)ratingRepository.findAll();

        Assert.assertEquals(result.size(), 0);
    }
    */
}
