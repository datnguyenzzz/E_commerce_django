package vn.datnguyen.recommender.Repository;

import java.util.concurrent.CompletionStage;

import com.datastax.oss.driver.api.core.CqlSession;

public class RepositoryFactory {

    private CompletionStage<CqlSession> session; 

    public RepositoryFactory(CompletionStage<CqlSession> session) {
        this.session = session;
    }

    public CompletionStage<CqlSession> getSession() {
        return session;
    }
}
