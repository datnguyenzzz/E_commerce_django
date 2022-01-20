package vn.datnguyen.recommender.Cache.Repository;

import org.springframework.stereotype.Repository;
import org.springframework.data.repository.CrudRepository;

@Repository
public interface CachedEventRepository extends CrudRepository<CachedEvent, String> {
    
}
