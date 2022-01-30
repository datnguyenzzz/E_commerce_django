package vn.datnguyen.recommender.Repositories;

import java.util.List;

import org.springframework.data.repository.CrudRepository;

import vn.datnguyen.recommender.Models.CachedQuery;

public interface CachedQueryRepository extends CrudRepository<CachedQuery, Long> {
    List<CachedQuery> findByClientIdAndItemId(String clientId, String itemId);
}
