package vn.datnguyen.recommender.Controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import vn.datnguyen.recommender.Models.Rating;
import vn.datnguyen.recommender.Repositories.RatingRepository;

@RestController
@RequestMapping("/api/v/1.0.0/rating")
public class ApiController {

    private RatingRepository ratingRepository;

    @Autowired
    public ApiController(RatingRepository ratingRepository) {
        this.ratingRepository = ratingRepository;
    }
    
    @GetMapping
    public ResponseEntity<List<Rating>> getRating(@RequestParam(required = false) String clientId, @RequestParam(required = false) String itemId) {
        if (clientId != null && itemId != null) {
            return ResponseEntity.status(HttpStatus.ACCEPTED).body(ratingRepository.findByClientIdAndItemId(clientId, itemId));
        }
        else if (clientId==null && itemId==null) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(null);
        }
        else if (clientId != null) {
            return ResponseEntity.status(HttpStatus.ACCEPTED).body(ratingRepository.findByClientId(clientId));
        }
        else {
            return ResponseEntity.status(HttpStatus.ACCEPTED).body(ratingRepository.findByItemId(itemId));
        }
    }

}
