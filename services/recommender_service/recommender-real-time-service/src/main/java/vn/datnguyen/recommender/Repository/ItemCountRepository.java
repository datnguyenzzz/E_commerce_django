package vn.datnguyen.recommender.Repository;

import com.datastax.oss.driver.api.core.CqlSession;

public class ItemCountRepository implements ItemCountInterface {
    private CqlSession session; 

    public ItemCountRepository(CqlSession session) {
        this.session = session;
    }

    public CqlSession getSession() {
        return session;
    }
}
