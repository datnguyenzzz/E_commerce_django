package vn.datnguyen.recommender.Repository;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;

import vn.datnguyen.recommender.Models.ClientRating;

public interface ClientRatingInterface {
    SimpleStatement createRowIfNotExists();
    SimpleStatement createIndexOnItemId();
    SimpleStatement findByClientIdAndItemId(String clientId, String itemId);
    SimpleStatement insertClientRating(ClientRating clientRating);
    SimpleStatement updateIfGreaterClientRating(ClientRating clientRating);
}
