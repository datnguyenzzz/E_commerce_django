package vn.datnguyen.recommender.Repository;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;

import vn.datnguyen.recommender.Models.ItemCount;

public interface ItemCountInterface {
    SimpleStatement createRowIfNotExists();
    SimpleStatement findByItemId(String itemId);
    SimpleStatement insertNewScore(ItemCount itemcount);
    SimpleStatement updateIncrScore(String itemId, int deltaScore);
}
