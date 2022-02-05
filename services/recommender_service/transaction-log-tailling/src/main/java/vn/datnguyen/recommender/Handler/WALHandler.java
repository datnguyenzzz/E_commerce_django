package vn.datnguyen.recommender.Handler;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import vn.datnguyen.recommender.AvroClasses.AvroAddToCartBehavior;
import vn.datnguyen.recommender.AvroClasses.AvroBuyBehavior;
import vn.datnguyen.recommender.AvroClasses.AvroDeleteRating;
import vn.datnguyen.recommender.AvroClasses.AvroEvent;
import vn.datnguyen.recommender.AvroClasses.AvroPublishRating;
import vn.datnguyen.recommender.AvroClasses.AvroQueryRating;
import vn.datnguyen.recommender.AvroClasses.AvroUpdateRating;

@Component
public class WALHandler {

    @Value("${transactionKafka.partitionIdCommandRating}")
    private String partitionIdCommandRating;

    @Value("${transactionKafka.partitionIdQueryRating}")
    private String partitionIdQueryRating;

    @Value("${transactionKafka.partitionIdBuyBehavior}")
    private String partitionIdBuyBehavior;

    @Value("${transactionKafka.partitionIdAddToCartBehavior}")
    private String partitionIdAddToCartBehavior;

    @Value("${incomingEvent.avroPublishRatingEvent}")
    private String avroPublishRatingEvent;

    @Value("${incomingEvent.avroUpdateRatingEvent}")
    private String avroUpdateRatingEvent;

    @Value("${incomingEvent.avroDeleteRatingEvent}")
    private String avroDeleteRatingEvent;

    @Value("${incomingEvent.avroQueryRatingEvent}")
    private String avroQueryRatingEvent;

    @Value("${incomingEvent.avroBuyBehaviorEvent}")
    private String avroBuyBehaviorEvent;

    @Value("${incomingEvent.avroAddToCartBehaviorEvent}")
    private String avroAddToCartBehaviorEvent;

    @Value("${DBTable.clientIdCol}")
    private String clientIdCol;

    @Value("${DBTable.itemIdCol}")
    private String itemIdCol;

    @Value("${DBTable.scoreCol}")
    private String scoreCol;

    @Value("${DBTable.eventTypeCol}")
    private String eventTypeCol;

    private final Logger logger = LoggerFactory.getLogger(WALHandler.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final TransactionalPublisher eventPublisher;

    @Autowired
    public WALHandler(TransactionalPublisher eventPublisher) {
        this.eventPublisher = eventPublisher;
    }

    public CompletableFuture<Void> process(String wal) {
        return CompletableFuture.runAsync(
            () -> 
                Stream.of(wal)
                    .map(this::splitBySpace)
                    .filter(this::isContainedData)
                    .filter(this::isFromOutboxTable)
                    .filter(this::isInsert)
                    .map(this::getPayload)
                    .map(this::toAvroEvent)
                    .forEach(eventPublisher::execute)

        );
    }

    private String[] splitBySpace(String wal) {
        return wal.split(" ");
    }

    private boolean isContainedData(String[] wal) {
        // wal contained data alway has more than 1 element
        logger.debug("WAL-MINER" + wal);
        return (wal.length > 1);
    }

    private boolean isFromOutboxTable(String[] wal) {
        //wal[1] is information about table 
        String outboxTable = "public.outbox:";
        logger.debug("WAL-MINER" + wal[1]);
        return (wal[1].equals(outboxTable));
    }

    private boolean isInsert(String[] wal) {
        //wal[2] is information about method 
        String insertMethod = "INSERT:";
        logger.debug("WAL-MINER" + wal[2]);
        return (wal[2].equals(insertMethod));
    }

    private Map<String, Object> getPayload(String[] wal) {
        try {
            String walPayload = wal[wal.length - 1];
            int firstPos = walPayload.indexOf('{');
            int lastPos = walPayload.lastIndexOf('}');
            walPayload =walPayload.substring(firstPos, lastPos+1);

            Map<String, Object> payload = 
                objectMapper.readValue(walPayload, new TypeReference<Map<String, Object>>(){} );

            return payload;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private AvroEvent toAvroEvent(Map<String, Object> walPayload) {
        String eventType = (String)walPayload.get(eventTypeCol);
        Object payload = null;
        if (eventType.equals(avroPublishRatingEvent)) {
            payload = (AvroPublishRating)AvroPublishRating.newBuilder()
                        .setClientId((String)walPayload.get(clientIdCol))
                        .setItemId((String)walPayload.get(itemIdCol))
                        .setScore((int)walPayload.get(scoreCol))
                        .build();
        }
        else if (eventType.equals(avroUpdateRatingEvent)) {
            payload = (AvroUpdateRating)AvroUpdateRating.newBuilder()
                        .setClientId((String)walPayload.get(clientIdCol))
                        .setItemId((String)walPayload.get(itemIdCol))
                        .setScore((int)walPayload.get(scoreCol))
                        .build();
        }
        else if (eventType.equals(avroDeleteRatingEvent)) {
            payload = (AvroDeleteRating)AvroDeleteRating.newBuilder()
                        .setClientId((String)walPayload.get(clientIdCol))
                        .setItemId((String)walPayload.get(itemIdCol))
                        .build();
        }
        else if (eventType.equals(avroQueryRatingEvent)) {
            payload = (AvroQueryRating)AvroQueryRating.newBuilder()
                        .setClientId((String)walPayload.get(clientIdCol))
                        .setItemId((String)walPayload.get(itemIdCol))
                        .build();
        }
        else if (eventType.equals(avroBuyBehaviorEvent)) {
            payload = (AvroBuyBehavior)AvroBuyBehavior.newBuilder()
                        .setClientId((String)walPayload.get(clientIdCol))
                        .setItemId((String)walPayload.get(itemIdCol))
                        .build();
        }
        else if (eventType.equals(avroAddToCartBehaviorEvent)) {
            payload = (AvroAddToCartBehavior)AvroAddToCartBehavior.newBuilder()
                        .setClientId((String)walPayload.get(clientIdCol))
                        .setItemId((String)walPayload.get(itemIdCol))
                        .build();
        }

        return wrapper(payload, eventType);
    }

    private AvroEvent wrapper(Object payload, String eventType) {
        int partitionId = 0; 

        if (eventType.equals(avroUpdateRatingEvent) || 
            eventType.equals(avroPublishRatingEvent) ||
            eventType.equals(avroDeleteRatingEvent)) {
                partitionId = Integer.parseInt(partitionIdCommandRating);
        }
        else if (eventType.equals(avroQueryRatingEvent)) {
            partitionId = Integer.parseInt(partitionIdQueryRating);
        }
        else if (eventType.equals(avroBuyBehaviorEvent)) {
            partitionId = Integer.parseInt(partitionIdBuyBehavior);
        }

        else if (eventType.equals(avroAddToCartBehaviorEvent)) {
            partitionId = Integer.parseInt(partitionIdAddToCartBehavior);
        }

        return AvroEvent.newBuilder()
                        .setEventId(UUID.randomUUID().toString())
                        .setPartitionId(partitionId)
                        .setTimestamp(System.currentTimeMillis())
                        .setEventType(eventType)
                        .setData(payload)
                        .build();
    }
}
