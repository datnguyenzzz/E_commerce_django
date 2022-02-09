package vn.datnguyen.recommender.Repository;


import com.datastax.oss.driver.api.core.CqlSession;

public class RepositoryFactory {

    private CqlSession session;

    public RepositoryFactory(CqlSession session) {
        this.session = session;
    }

    public KeyspaceRepository getKeyspaceRepository() {
        return new KeyspaceRepository(session);
    }

    public UserRatingRepository getUserRatingRepository() {
        return new UserRatingRepository(session);
    }
}
