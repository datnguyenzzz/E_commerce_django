package vn.datnguyen.recommender.Models;

import java.io.Serializable;
import java.util.List;

import javax.annotation.Nullable;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;

import org.hibernate.annotations.GenericGenerator;
import org.springframework.data.annotation.Id;
import org.springframework.data.redis.core.RedisHash;

@RedisHash(value = "Query", timeToLive = 3600)
public class CachedQuery implements Serializable {

    private long queryId;
    private String clientId; 
    private String itemId;
    private List<Rating> result;

    public CachedQuery(String clientId, String itemId) {
        this.clientId = clientId;
        this.itemId = itemId;
    }

    public CachedQuery(String clientId, String itemId, List<Rating> result) {
        this(clientId, itemId);
        this.result = result;
    }

    public CachedQuery(long queryId, String clientId, String itemId, List<Rating> result) {
        this(clientId, itemId, result);
        this.queryId = queryId;
    }

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO, generator = "seq")
    @GenericGenerator(name = "seq", strategy = "increment")
    public long getQueryId() {
        return this.queryId;
    }

    @Nullable
    public String getClientId() {
        return this.clientId;
    }

    @Nullable
    public String getItemId() {
        return this.itemId;
    }

    @Nullable
    public List<Rating> getResult() {
        return this.result;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    public void setResult(List<Rating> result) {
        this.result = result;
    }

    public void setQueryId(long queryId) {
        this.queryId = queryId;
    }

}
