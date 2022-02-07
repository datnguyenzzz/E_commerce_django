package vn.datnguyen.recommender.Controller;

import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import vn.datnguyen.recommender.Domain.AddToCartBehaviorCommand;
import vn.datnguyen.recommender.Domain.BuyBehaviorCommand;
import vn.datnguyen.recommender.Domain.DeleteRatingCommand;
import vn.datnguyen.recommender.Domain.PublishRatingCommand;
import vn.datnguyen.recommender.Domain.UpdateRatingCommand;
import vn.datnguyen.recommender.Handler.RatingService;

@RestController
@RequestMapping("/api/v/1.0.0")
public class ApiController {

    private final Logger logger = LoggerFactory.getLogger(ApiController.class);

    private RatingService ratingService;

    @Autowired
    public ApiController(RatingService ratingService) {
        this.ratingService = ratingService;
    }

    @PostMapping("/testing/rating")
    public CompletableFuture<ResponseEntity<String>> PublishRating(@Validated @RequestBody PublishRatingCommand command) {
        logger.info("TESTING-RATING-SERVICE: " + "published rating command = " + command.toString());
        return ratingService.process(command)
                            .thenApply(result -> {
                                String bodyRes = "Published sucessfully " + command.toString();
                                return ResponseEntity.status(HttpStatus.ACCEPTED).body(bodyRes);
                            })
                            .exceptionally(e -> {
                                logger.warn("COMMAND-RATING-SERVICE: "+ "error when publish event on publish command"+ e);
                                String bodyRes = "Published not sucessfully " + command.toString();
                                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(bodyRes);
                            });
    }

}
