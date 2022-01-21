package vn.datnguyen.recommender.Utils;

import java.io.IOException;
import java.util.Map;

import javax.persistence.AttributeConverter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutboxHashMapConverter implements AttributeConverter<Map<String, Object>, String> {

    private static final Logger logger = LoggerFactory.getLogger(EventHashMapConverter.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public String convertToDatabaseColumn(Map<String, Object> payloadInfo) {
        String payloadJSON = null;

        try {
            payloadJSON = objectMapper.writeValueAsString(payloadInfo);
        } catch (JsonProcessingException e) {
            logger.error("EVENT-SOURCE: json outbox db writting error",e);
        }

        return payloadJSON;
    }

    @Override
    public Map<String, Object> convertToEntityAttribute(String payloadJSON) {
        Map<String, Object> payload = null;

        try {
            payload = objectMapper.readValue(payloadJSON, new TypeReference<Map<String, Object>>(){});
        } catch (IOException e) {
            logger.error("EVENT-SOURCE: json outbox db reading error",e);
        }
        
        return payload;
    }
}
