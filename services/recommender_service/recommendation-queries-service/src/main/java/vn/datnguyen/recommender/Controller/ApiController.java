package vn.datnguyen.recommender.Controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v/1.0.0")
public class ApiController {
    private final Logger logger = LoggerFactory.getLogger(ApiController.class);

    @GetMapping("/learn")
    public String getRecommendationsForItem() {
        logger.info("RECOMMENDATION-QUERIES-SERVICE: Try get recommendation for item");
        return "this is result";
    }
}
