package vn.datnguyen.recommender.Configurations;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.socialsignin.spring.data.dynamodb.repository.config.EnableDynamoDBRepositories;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import vn.datnguyen.recommender.Repositories.RatingRepository;

@Configuration
@EnableDynamoDBRepositories(basePackageClasses = RatingRepository.class)
public class DynamoDBConfiguration {

    private final Logger logger = LoggerFactory.getLogger(DynamoDBConfiguration.class);
    
    @Value("${amazon.dynamodb.endpoint}")
    private String dynamoDBEndpoint;

    @Value("${amazon.aws.accesskey}")
    private String awsAccessKey;

    @Value("${amazon.aws.secretkey}")
    private String awsSecretKey;

    @Bean
    public AWSCredentialsProvider amazonAwsCredentialsProvider() {
        AWSCredentials credentials = new  BasicAWSCredentials(awsAccessKey, awsSecretKey);
        return new AWSStaticCredentialsProvider(credentials);
    }

    @Bean
    public AmazonDynamoDB amazonDynamoDB(@Autowired AWSCredentialsProvider amazonAwsCredentialsProvider) {
        logger.info("-----TEST------: " + dynamoDBEndpoint);
        return AmazonDynamoDBClientBuilder.standard()
                .withCredentials(amazonAwsCredentialsProvider)
                .withEndpointConfiguration(
                    new AwsClientBuilder.EndpointConfiguration(
                        dynamoDBEndpoint, Regions.EU_NORTH_1.getName()))
                .build();
    }

}
