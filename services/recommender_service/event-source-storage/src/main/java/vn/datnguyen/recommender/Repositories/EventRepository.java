package vn.datnguyen.recommender.Repositories;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import vn.datnguyen.recommender.Models.EventEntity;

@Repository
public interface EventRepository extends JpaRepository<EventEntity, Long> {
    //functions
}
