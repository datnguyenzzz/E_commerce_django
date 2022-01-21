package vn.datnguyen.recommender.Repositories;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import vn.datnguyen.recommender.Models.OutboxEntity;

@Repository
public interface OutboxRepository extends JpaRepository<OutboxEntity, Long> {
    
}
