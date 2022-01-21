package vn.datnguyen.recommender.Models;

import java.io.IOException;
import java.util.Map;

import javax.persistence.Column;
import javax.persistence.Convert;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.hibernate.annotations.GenericGenerator;

import vn.datnguyen.recommender.Utils.EventHashMapConverter;


@Entity
@Table(name = "EVENT")
public class EventEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO, generator = "seq")
    @GenericGenerator(name = "seq", strategy = "increment")
    @Column(name = "ID")
    private long eventId;

    @Column(name = "TYPE")
    private String eventType;

    @Column(name = "DATA")
    private String payloadJSON;
    
    // Object -> String before persist 
    // String -> Object when get
    @Convert(converter = EventHashMapConverter.class)
    private Map<String, Object> payload;

    public long getEventId() {
        return this.eventId;
    }

    public void setEventId(long eventId) {
        this.eventId = eventId;
    }

    public String getEventType() {
        return this.eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getPayloadJSON() {
        return this.payloadJSON;
    }

    public void setPayloadJSON(String payloadJSON) {
        this.payloadJSON = payloadJSON;
    }

    public Map<String, Object> getPayload() {
        return this.payload;
    }

    public void setPayload(Map<String, Object> payload) {
        this.payload = payload;
    }

    // Object mapper 
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    public void serializePayload() throws JsonProcessingException {
        String jsonResult = objectMapper.writeValueAsString(getPayload());
        setPayloadJSON(jsonResult);
    }

    public void deserializePayload() throws IOException {
        Map<String, Object> mapResult = 
            objectMapper.readValue(getPayloadJSON(), new TypeReference<Map<String, Object>>(){});
        
        setPayload(mapResult);
    }
}
