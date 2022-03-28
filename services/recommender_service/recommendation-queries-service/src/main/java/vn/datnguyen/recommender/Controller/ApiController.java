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

import vn.datnguyen.recommender.AvroClasses.Item;
import vn.datnguyen.recommender.AvroClasses.RecommendItemSimilaritesResult;
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

    private String beautyWrite(Item item) {
        return "\n\t[\n"
            + "\t\titemId: " + item.getItemId() + ",\n"
            + "\t\tdifference: " + item.getDifference()
            + "\n\t],\n";
    }

    private String beautyWrite(RecommendItemSimilaritesResult result) {
        String res = "[\n"
            + "\tresultId: " + result.getId() + ",\n"
            + "\ttimeStamp: " + result.getTimestamp() + ",";
            
        for (Item item: result.getSimilarities()) {
            res += beautyWrite(item);
        }

        res = res.substring(0, res.length()-1);
        res += "\n]";
        return res;
    }

    @GetMapping("/learn")
    public ResponseEntity<String> getRecommendationsForItem(@Validated @RequestBody RecommendationForItemCommand command) {
        logger.info("RECOMMENDATION-QUERIES-SERVICE: Try get recommendation for item - " + command);
        CompletableFuture<Object> futureResult = service.process(command);

        RecommendItemSimilaritesResult result;
        try {
            result = (RecommendItemSimilaritesResult) futureResult.get();
        } 
        catch (Exception ex) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ex.getMessage());
        }

        return ResponseEntity.status(HttpStatus.ACCEPTED).body(beautyWrite(result));

    }
}
