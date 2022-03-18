package vn.datnguyen.recommender.Controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import vn.datnguyen.recommender.Domain.RecommendationForItemCommand;

@RestController
@RequestMapping("/api/v/1.0.0")
public class ApiController {
    private final Logger logger = LoggerFactory.getLogger(ApiController.class);

    @GetMapping("/learn")
    public String getRecommendationsForItem(@Validated @RequestBody RecommendationForItemCommand command) {
        logger.info("RECOMMENDATION-QUERIES-SERVICE: Try get recommendation for item - " + command);
        return "this is result";
    }
}
