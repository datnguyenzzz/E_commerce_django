package vn.datnguyen.recommender;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import junit.framework.Assert;
import vn.datnguyen.recommender.Handlers.CommandServicesImpl;
import vn.datnguyen.recommender.Handlers.QueryServicesImpl;
import vn.datnguyen.recommender.Models.Rating;

/**
 * Unit test for simple App.
 */
@SpringBootTest(classes = App.class)
@TestPropertySource(properties = {
    "amazon.dynamodb.endpoint=http://localhost:8000/",
    "amazon.aws.accesskey=test-access", 
    "amazon.aws.secretkey=test-secret",
    "ConsumerKafka.bootstrapServers= http://localhost:9093",
    "ConsumerKafka.groupId=fromEventSourcing",
    "ConsumerKafka.topicConsumerFromEventSource=ratingCommandFromEventSource"
})
public class AppTest {

    @Autowired
    private QueryServicesImpl queryServicesImpl;

    @Autowired
    private CommandServicesImpl commandServicesImpl;

    @Test
    public void testFindingById_thenDelete() {
        String clientId = "321-451-312";
        String itemId = "320-312-512";
        int score = 213;

        Rating rating = new Rating(clientId, itemId, score);

        List<Rating> result = queryServicesImpl.findAllRating();
        int prevSize = result.size();

        commandServicesImpl.addNewClientRating(rating);

        result = queryServicesImpl.findAllRating();
        Assert.assertTrue(result.size() > prevSize);
        Assert.assertEquals(result.get(result.size() - 1).getClientId(), clientId);
        Assert.assertEquals(result.get(result.size() - 1).getItemId(), itemId);
        Assert.assertEquals(result.get(result.size() - 1).getScore(), score);

        commandServicesImpl.deleteClientRating(rating);

        result = queryServicesImpl.findAllRating();
        Assert.assertTrue(result.size() == prevSize);
    }
}
