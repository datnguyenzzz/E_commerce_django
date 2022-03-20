package vn.datnguyen.recommender.Handler;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import vn.datnguyen.recommender.AvroClasses.AvroEvent;
import vn.datnguyen.recommender.AvroClasses.AvroRecommendForItem;
import vn.datnguyen.recommender.Domain.Command;
import vn.datnguyen.recommender.Domain.RecommendationForItemCommand;

@Service
public class QueryRecommendationService implements CommandHandler {

    @Value("${incomingEvent.avroRecommendForItemEvent}")
    private String avroRecommendForItemEvent;

    private ReplyingKafkaService kafkaService;
    
    @Autowired
    public QueryRecommendationService(ReplyingKafkaService kafkaService) {
        this.kafkaService = kafkaService;
    }

    @Override
    public CompletableFuture<Object> process(Command command) {
        return CompletableFuture.supplyAsync(
            () -> {
                AvroEvent event = toAvroEvent(command);
                Object result; 
                try {
                    result = this.kafkaService.requestThenReply(event);
                }
                catch (Exception ex) {
                    result = ex.getMessage();
                }

                return result;
            }
        );
    }

    private AvroEvent toAvroEvent(Command command) {
        if (command instanceof RecommendationForItemCommand) {
            return toAvroEvent((RecommendationForItemCommand) command);
        }
        return null;
    }

    private AvroEvent toAvroEvent(RecommendationForItemCommand command) {
        AvroRecommendForItem eventPayload = AvroRecommendForItem.newBuilder()
            .setItemId(command.getItemId())
            .setLimit(command.getLimit())
            .setProperties1(command.getProperty1())
            .setProperties2(command.getProperty2())
            .setProperties3(command.getProperty3())
            .build();
        
        return wrap(eventPayload, avroRecommendForItemEvent);
    }

    private AvroEvent wrap(Object payload, String payloadType) {

        return AvroEvent.newBuilder()
                        .setEventId(UUID.randomUUID().toString())
                        .setPartitionId(-1)
                        .setTimestamp(Long.toString(System.currentTimeMillis()))
                        .setEventType(payloadType)
                        .setData(payload)
                        .build();
    }
}
