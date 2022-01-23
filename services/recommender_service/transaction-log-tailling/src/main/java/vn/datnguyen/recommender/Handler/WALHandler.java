package vn.datnguyen.recommender.Handler;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class WALHandler {

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

    private void publish(Map<String, Object> walPayload) {
        logger.info("START publishing payload = " + walPayload);
    }
}
