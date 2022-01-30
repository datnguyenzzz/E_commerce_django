package vn.datnguyen.recommender.Controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import vn.datnguyen.recommender.Models.CachedQuery;
import vn.datnguyen.recommender.Models.Rating;
import vn.datnguyen.recommender.Repositories.CachedQueryRepository;
import vn.datnguyen.recommender.Repositories.RatingRepository;

@RestController
@RequestMapping("/api/v/1.0.0/rating")
public class ApiController {

    private RatingRepository ratingRepository;
    private CachedQueryRepository cachedQueryRepository;

    @Autowired
    public ApiController(RatingRepository ratingRepository, CachedQueryRepository cachedQueryRepository) {
        this.ratingRepository = ratingRepository;
        this.cachedQueryRepository = cachedQueryRepository;
    }
    
    @GetMapping
    public ResponseEntity<List<Rating>> getRating(@RequestParam(required = false) String clientId, @RequestParam(required = false) String itemId) {
        if (clientId != null && itemId != null) {

            List<Rating> result;
            List<CachedQuery> cachedQuery = cachedQueryRepository.findByClientIdAndItemId(clientId, itemId);

            if (cachedQuery.size() == 1) {
                result = cachedQuery.get(0).getResult();
            }
            else {
                result = ratingRepository.findByClientIdAndItemId(clientId, itemId);
                CachedQuery cached = new CachedQuery(clientId, itemId, result);
                cachedQueryRepository.save(cached);
            }

            return ResponseEntity.status(HttpStatus.ACCEPTED).body(result);
        }
        else if (clientId==null && itemId==null) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(null);
        }
        else if (clientId != null) {

            List<Rating> result;
            List<CachedQuery> cachedQuery = cachedQueryRepository.findByClientIdAndItemId(clientId,null);

            if (cachedQuery.size() == 1) {
                result = cachedQuery.get(0).getResult();
            }
            else {
                result = ratingRepository.findByClientIdAndItemId(clientId, null);
                CachedQuery cached = new CachedQuery(clientId, null, result);
                cachedQueryRepository.save(cached);
            }

            return ResponseEntity.status(HttpStatus.ACCEPTED).body(result);
        }
        else {
            List<Rating> result;
            List<CachedQuery> cachedQuery = cachedQueryRepository.findByClientIdAndItemId(null,itemId);

            if (cachedQuery.size() == 1) {
                result = cachedQuery.get(0).getResult();
            }
            else {
                result = ratingRepository.findByClientIdAndItemId(null,itemId);
                CachedQuery cached = new CachedQuery(null,itemId, result);
                cachedQueryRepository.save(cached);
            }

            return ResponseEntity.status(HttpStatus.ACCEPTED).body(result);
        }
    }

}
