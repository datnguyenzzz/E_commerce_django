package vn.datnguyen.recommender.Repositories;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import vn.datnguyen.recommender.Models.CachedEvent;

@Repository
public interface CachedEventRepository extends CrudRepository<CachedEvent, String> {
    
}
