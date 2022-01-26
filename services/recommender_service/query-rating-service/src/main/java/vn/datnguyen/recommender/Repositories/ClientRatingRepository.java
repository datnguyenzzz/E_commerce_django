package vn.datnguyen.recommender.Repositories;

import org.socialsignin.spring.data.dynamodb.repository.EnableScan;
import org.springframework.data.repository.CrudRepository;

import vn.datnguyen.recommender.Models.Rating;

@EnableScan()
public interface ClientRatingRepository extends CrudRepository<Rating, String> {
}
