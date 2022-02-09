package vn.datnguyen.recommender.Repository;

import java.util.concurrent.CompletionStage;

import com.datastax.oss.driver.api.core.CqlSession;

public class RepositoryFactory {

    private CompletionStage<CqlSession> sessionStage; 
    private CqlSession session;

    public RepositoryFactory(CqlSession session, CompletionStage<CqlSession> sessionStage) {
        this.session = session;
        this.sessionStage = sessionStage;
    }

    public KeyspaceRepository getKeyspaceRepository() {
        return new KeyspaceRepository(session, sessionStage);
    }

    public UserRatingRepository getUserRatingRepository() {
        return new UserRatingRepository();
    }
}
