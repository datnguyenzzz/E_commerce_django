package vn.datnguyen.recommender.Models;

import java.util.Map;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.hibernate.annotations.GenericGenerator;


@Entity
@Table(name = "OUTBOX")
public class OutboxEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO, generator = "seq")
    @GenericGenerator(name = "seq", strategy = "increment")
    @Column(name = "ID")
    private long eventId;

    @Column(name = "DATA")
    private String payloadJSON;

    public long getEventId() {
        return this.eventId;
    }

    public void setEventId(long eventId) {
        this.eventId = eventId;
    }

    public String getPayloadJSON() {
        return this.payloadJSON;
    }

    public void setPayloadJSON(String payloadJSON) {
        this.payloadJSON = payloadJSON;
    }

    public void setPayloadJSON(Map<String, Object> payload) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        String outboxJson = objectMapper.writeValueAsString(payload);

        setPayloadJSON(outboxJson);
    }
}
