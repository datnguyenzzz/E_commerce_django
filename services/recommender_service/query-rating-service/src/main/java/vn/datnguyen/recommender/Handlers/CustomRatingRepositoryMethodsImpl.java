package vn.datnguyen.recommender.Handlers;

import java.util.List;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;

import org.springframework.beans.factory.annotation.Autowired;

import vn.datnguyen.recommender.Models.Rating;

public class CustomRatingRepositoryMethodsImpl implements CustomRatingRepositoryMethods {
    
    private AmazonDynamoDB amazonDynamoDB;
    private DynamoDBMapper dbMapper;

    @Autowired
    public CustomRatingRepositoryMethodsImpl(AmazonDynamoDB amazonDynamoDB) {
        this.amazonDynamoDB = amazonDynamoDB;
        this.dbMapper = new DynamoDBMapper(amazonDynamoDB);
    }

    public AmazonDynamoDB getAmazonDynamoDB() {
        return this.amazonDynamoDB;
    }
    
    @Override
    public List<Rating> findByClientId(String clientId) {
        return null;
    }

    @Override
    public List<Rating> findByItemId(String itemId) {
        return null;
    }

    @Override
    public Rating findByClientIdAndItemId(String clientId, String itemId) {
        return null;
    }
    
}
