package vn.datnguyen.recommender.Models;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import org.hibernate.annotations.GenericGenerator;


@Entity
@Table(name = "OUTBOX")
public class OutboxEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO, generator = "seq")
    @GenericGenerator(name = "seq", strategy = "increment")
    @Column(name = "ID")
    private long eventId;

    @Column(name = "TYPE")
    private String eventType;

    @Column(name = "DATA")
    private String payloadJSON;

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


}
