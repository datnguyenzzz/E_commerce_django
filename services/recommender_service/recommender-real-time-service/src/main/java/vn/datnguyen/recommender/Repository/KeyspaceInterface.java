package vn.datnguyen.recommender.Repository;

import java.util.concurrent.CompletionStage;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;

public interface KeyspaceInterface {
    /**
     *  @param KeyspaceName
     *  @param NumberOfNodeReplicas
     *  @return CompletionStage useKeyspaceStage
     */
    public CompletionStage<AsyncResultSet> createAndUseKeyspace(String name, int numReplicas);
}
