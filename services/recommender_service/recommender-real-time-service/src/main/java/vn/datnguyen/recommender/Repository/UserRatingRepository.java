package vn.datnguyen.recommender.Repository;

import com.datastax.oss.driver.api.core.CqlSession;

public class UserRatingRepository {
    
    private CqlSession session; 

    public UserRatingRepository(CqlSession session) {
        this.session = session;
    }

    public CqlSession getSession() {
        return session;
    }
}
