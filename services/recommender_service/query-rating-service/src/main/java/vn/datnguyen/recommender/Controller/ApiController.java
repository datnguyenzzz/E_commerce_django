package vn.datnguyen.recommender.Controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import vn.datnguyen.recommender.Handlers.QueryRatingService;
import vn.datnguyen.recommender.Models.Rating;

@RestController
@RequestMapping("/api/v/1.0.0/rating")
public class ApiController {

    private QueryRatingService queryRatingService;

    @Autowired
    public ApiController(QueryRatingService queryRatingService) {
        this.queryRatingService = queryRatingService;
    }
    
    @GetMapping
    public ResponseEntity<List<Rating>> getRating(@RequestParam(required = false) String clientId, @RequestParam(required = false) String itemId) {
        if (clientId != null && itemId != null) {

            List<Rating> result = queryRatingService.process(clientId, itemId);

            return ResponseEntity.status(HttpStatus.ACCEPTED).body(result);
        }
        else if (clientId==null && itemId==null) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(null);
        }
        else if (clientId != null) {

            List<Rating> result = queryRatingService.process(clientId, null);
            return ResponseEntity.status(HttpStatus.ACCEPTED).body(result);
        }
        else {
            List<Rating> result = queryRatingService.process(null, itemId);
            return ResponseEntity.status(HttpStatus.ACCEPTED).body(result);
        }
    }

}
