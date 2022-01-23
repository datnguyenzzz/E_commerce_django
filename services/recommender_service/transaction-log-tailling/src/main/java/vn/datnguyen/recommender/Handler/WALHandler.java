package vn.datnguyen.recommender.Handler;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import vn.datnguyen.recommender.AvroClasses.AvroDeleteRating;
import vn.datnguyen.recommender.AvroClasses.AvroEvent;
import vn.datnguyen.recommender.AvroClasses.AvroPublishRating;
import vn.datnguyen.recommender.AvroClasses.AvroUpdateRating;

@Component
public class WALHandler {

    @Value("${transactionKafka.messageId}")
    private String partitionId;

    @Value("${incomingEvent.avroPublishRatingEvent}")
    private String avroPublishRatingEvent;

    @Value("${incomingEvent.avroUpdateRatingEvent}")
    private String avroUpdateRatingEvent;

    @Value("${incomingEvent.avroDeleteRatingEvent}")
    private String avroDeleteRatingEvent;

    private final Logger logger = LoggerFactory.getLogger(WALHandler.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    public WALHandler() {}

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
                    .forEach(this::publish)

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
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private AvroEvent toAvroEvent(Map<String, Object> walPayload) {
        String eventType = (String)walPayload.get("eventType");
        Object payload = null;
        if (eventType.equals(avroPublishRatingEvent)) {
            payload = (AvroPublishRating)AvroPublishRating.newBuilder()
                        .setClientId((String)walPayload.get("clientId"))
                        .setItemId((String)walPayload.get("itemId"))
                        .setScore((int)walPayload.get("score"))
                        .build();
        }
        else if (eventType.equals(avroUpdateRatingEvent)) {
            payload = (AvroUpdateRating)AvroUpdateRating.newBuilder()
                        .setClientId((String)walPayload.get("clientId"))
                        .setItemId((String)walPayload.get("itemId"))
                        .setScore((int)walPayload.get("score"))
                        .build();
        }
        else {
            payload = (AvroDeleteRating)AvroDeleteRating.newBuilder()
                        .setClientId((String)walPayload.get("clientId"))
                        .setItemId((String)walPayload.get("itemId"))
                        .build();
        }

        return wrapper(payload, eventType);
    }

    private AvroEvent wrapper(Object payload, String eventType) {
        return AvroEvent.newBuilder()
                        .setEventId(UUID.randomUUID().toString())
                        .setPartitionId(Integer.parseInt(partitionId))
                        .setTimestamp(System.currentTimeMillis())
                        .setEventType(eventType)
                        .setData(payload)
                        .build();
    }

    private void publish(AvroEvent walPayload) {
        logger.info("START publishing payload = " + walPayload);
    }
}
