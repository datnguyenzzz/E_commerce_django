package vn.datnguyen.recommender.Repositories;

import org.springframework.stereotype.Repository;

import vn.datnguyen.recommender.Models.CachedEvent;

import org.springframework.data.repository.CrudRepository;

@Repository
public interface CachedEventRepository extends CrudRepository<CachedEvent, String> {
    
}
