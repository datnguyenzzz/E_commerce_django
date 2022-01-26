package vn.datnguyen.recommender;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.util.TableUtils;

import org.socialsignin.spring.data.dynamodb.repository.config.EnableDynamoDBRepositories;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;

import vn.datnguyen.recommender.Models.Rating;

/**
 * Hello world!
 *
 */
@SpringBootApplication
@EnableAutoConfiguration(exclude = {DataSourceAutoConfiguration.class, // No JPA
    DataSourceTransactionManagerAutoConfiguration.class, 
    HibernateJpaAutoConfiguration.class})
@EnableDynamoDBRepositories(basePackages = "vn.datnguyen.recommender.Repositories")
public class App implements CommandLineRunner {

    private AmazonDynamoDB amazonDynamoDB;

    @Autowired
    public App(AmazonDynamoDB amazonDynamoDB) {
        this.amazonDynamoDB = amazonDynamoDB;
    }

    public static void main( String[] args ) {
        SpringApplication.run(App.class, args);
    }

    @Override
    public void run(String... args) throws InterruptedException {
        DynamoDBMapper dynamoDBMapper = new DynamoDBMapper(amazonDynamoDB);
        CreateTableRequest ctr = dynamoDBMapper.generateCreateTableRequest(Rating.class)
            .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L));
        
        TableUtils.createTableIfNotExists(amazonDynamoDB, ctr);
        TableUtils.waitUntilActive(amazonDynamoDB, ctr.getTableName());
    }

}
