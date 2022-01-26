package vn.datnguyen.recommender;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import junit.framework.Assert;
import vn.datnguyen.recommender.Handlers.QueryServicesImpl;
import vn.datnguyen.recommender.Models.Rating;

/**
 * Unit test for simple App.
 */
@SpringBootTest(classes = App.class)
@TestPropertySource(properties = {
    "amazon.dynamodb.endpoint=http://localhost:8000/",
    "amazon.aws.accesskey=test-access", 
    "amazon.aws.secretkey=test-secret"
})
public class AppTest {

    @Autowired
    private QueryServicesImpl queryServicesImpl;

    @Test
    public void testFindingById_thenDelete() {
        String clientId = "321-451-312";
        String itemId = "320-312-512";
        int score = 213;

        Rating rating = new Rating(clientId, itemId, score);

        List<Rating> result = queryServicesImpl.findAllRating();
        int prevSize = result.size();

        queryServicesImpl.addNewRating(rating);

        result = queryServicesImpl.findAllRating();
        Assert.assertTrue(result.size() > prevSize);
        Assert.assertEquals(result.get(result.size() - 1).getClientId(), clientId);
        Assert.assertEquals(result.get(result.size() - 1).getItemId(), itemId);
        Assert.assertEquals(result.get(result.size() - 1).getScore(), score);

        queryServicesImpl.deleteRating(rating);

        result = queryServicesImpl.findAllRating();
        Assert.assertTrue(result.size() == prevSize);
    }
}
