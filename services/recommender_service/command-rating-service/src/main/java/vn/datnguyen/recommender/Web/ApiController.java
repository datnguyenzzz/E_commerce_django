package vn.datnguyen.recommender.Web;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v/1.0.0/rating")
public class ApiController {

    private final Logger logger = LoggerFactory.getLogger(ApiController.class);
    
    @GetMapping("/test")
    public String testApi() {
        logger.info("COMMAND-RATING-SERVICE" + "testing service");
        return "v.1.0.0";
    }
}
