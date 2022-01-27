package vn.datnguyen.recommender.Handlers;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.transaction.Transactional;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import org.springframework.beans.factory.annotation.Autowired;

import vn.datnguyen.recommender.Models.Rating;

public class RatingRepositoryCustomImpl implements RatingRepositoryCustom {
    
    private AmazonDynamoDB amazonDynamoDB;
    private DynamoDBMapper dbMapper;

    @Autowired
    public RatingRepositoryCustomImpl(AmazonDynamoDB amazonDynamoDB) {
        this.amazonDynamoDB = amazonDynamoDB;
        this.dbMapper = new DynamoDBMapper(amazonDynamoDB);
    }

    public AmazonDynamoDB getAmazonDynamoDB() {
        return this.amazonDynamoDB;
    }

    @Override
    public List<Rating> findByClientId(String clientId) {
        int ok = 1 ;
        return null;
    }

    @Override
    public List<Rating> findByItemId(String itemId) {
        int ok = 1 ;
        return null;
    }

    @Override
    public List<Rating> findByClientIdAndItemId(String clientId, String itemId) {
        Map<String, AttributeValue> eav = new HashMap<>();
        eav.put(":v1", new AttributeValue().withS(clientId)); 
        eav.put(":v2", new AttributeValue().withS(itemId)); 

        DynamoDBQueryExpression<Rating> queryExpression = new DynamoDBQueryExpression<Rating>()
            .withKeyConditionExpression("clientId = :v1 and itemId = :v2")
            .withExpressionAttributeValues(eav);
        
        List<Rating> ratings = dbMapper.query(Rating.class, queryExpression);

        assert ratings.size() <= 1; 

        return ratings;
    }
    
}
