package vn.datnguyen.recommender.Repositories;

import java.util.List;
import java.util.Optional;

import org.socialsignin.spring.data.dynamodb.repository.EnableScan;
import org.springframework.data.repository.CrudRepository;

import vn.datnguyen.recommender.Models.Rating;

@EnableScan()
public interface ClientRatingRepository extends CrudRepository<Rating, String> {

    Optional<Rating> findByClientIdAndItemId(String clientId, String itemId); 

    List<Rating> findByClientId(String clientId);

    List<Rating> findByItemId(String clientId);
}
