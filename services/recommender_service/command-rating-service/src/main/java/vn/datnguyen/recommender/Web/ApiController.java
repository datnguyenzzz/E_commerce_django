package vn.datnguyen.recommender.Web;

import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import vn.datnguyen.recommender.Domain.DeleteRatingCommand;
import vn.datnguyen.recommender.Domain.PublishRatingCommand;
import vn.datnguyen.recommender.Domain.UpdateRatingCommand;
import vn.datnguyen.recommender.Handler.RatingService;

@RestController
@RequestMapping("/api/v/1.0.0/rating")
public class ApiController {

    private final Logger logger = LoggerFactory.getLogger(ApiController.class);

    private RatingService ratingService;

    @Autowired
    public ApiController(RatingService ratingService) {
        this.ratingService = ratingService;
    }

    @PostMapping()
    public CompletableFuture<String> PublishRating(@Validated @RequestBody PublishRatingCommand command) {
        logger.info("COMMAND-RATING-SERVICE: " + "published rating command = " + command.toString());
        return ratingService.process(command)
                            .thenApply(result -> "Published successfully!!")
                            .exceptionally(e -> {
                                logger.warn("COMMAND-RATING-SERVICE: "+ "error when publish event on publish command", e);
                                return "Error when transfer publishing";
                            });
                                
    }

    @PutMapping() 
    public CompletableFuture<String> UpdateRating(@Validated @RequestBody UpdateRatingCommand command) {
        logger.info("COMMAND-RATING-SERVICE: " + "updated rating command = "+ command.toString());
        return ratingService.process(command)
                            .thenApply(result -> "Updated successfully!!")
                            .exceptionally(e -> {
                                logger.warn("COMMAND-RATING-SERVICE: "+ "error when publish event on publish command", e);
                                return "Error when transfer publishing";
                            });
    }

    @DeleteMapping() 
    public CompletableFuture<String> DeleteRating(@Validated @RequestBody DeleteRatingCommand command) {
        logger.info("COMMAND-RATING-SERVICE: " + "deleted rating command = "+ command.toString());
        return ratingService.process(command)
                            .thenApply(result -> "Deleted successfully!!")
                            .exceptionally(e -> {
                                logger.warn("COMMAND-RATING-SERVICE: "+ "error when publish event on publish command", e);
                                return "Error when transfer publishing";
                            });
    }
}
