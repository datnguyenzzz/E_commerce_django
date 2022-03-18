package vn.datnguyen.recommender.Controller;

import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import vn.datnguyen.recommender.Domain.RecommendationForItemCommand;
import vn.datnguyen.recommender.Handler.QueryRecommendationService;

@RestController
@RequestMapping("/api/v/1.0.0")
public class ApiController {
    private final Logger logger = LoggerFactory.getLogger(ApiController.class);
    private QueryRecommendationService service;

    @Autowired
    public ApiController(QueryRecommendationService queryRecommendationService) {
        this.service = queryRecommendationService;
    }

    @GetMapping("/learn")
    public ResponseEntity<String> getRecommendationsForItem(@Validated @RequestBody RecommendationForItemCommand command) {
        logger.info("RECOMMENDATION-QUERIES-SERVICE: Try get recommendation for item - " + command);
        CompletableFuture<String> futureResult = 
            service.process(command)
                   .thenApply(res -> {
                       return (String) res;
                   });

        String result;
        try {
            result = futureResult.get();
        } 
        catch (Exception ex) {
            result = ex.getMessage();
        }
        
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(result);

    }
}
