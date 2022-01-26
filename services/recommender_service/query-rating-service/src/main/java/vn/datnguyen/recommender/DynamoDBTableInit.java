package vn.datnguyen.recommender;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.util.TableUtils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;

import vn.datnguyen.recommender.Models.Rating;

public class DynamoDBTableInit implements ApplicationListener<ContextRefreshedEvent> {
    private AmazonDynamoDB amazonDynamoDB;

    @Autowired
    public DynamoDBTableInit(AmazonDynamoDB amazonDynamoDB) {
        this.amazonDynamoDB = amazonDynamoDB;
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        DynamoDBMapper dynamoDBMapper = new DynamoDBMapper(amazonDynamoDB);

        CreateTableRequest ctr = dynamoDBMapper.generateCreateTableRequest(Rating.class)
            .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L));
        
        TableUtils.createTableIfNotExists(amazonDynamoDB, ctr);
    }
}
