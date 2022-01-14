package vn.datnguyen.recommender.Web;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import vn.datnguyen.recommender.Web.commands.DeleteRatingCommand;
import vn.datnguyen.recommender.Web.commands.PublishRatingCommand;
import vn.datnguyen.recommender.Web.commands.UpdateRatingCommand;

@RestController
@RequestMapping("/api/v/1.0.0/rating")
public class ApiController {

    private final Logger logger = LoggerFactory.getLogger(ApiController.class);
    
    @GetMapping("/test")
    public String testApi() {
        logger.info("COMMAND-RATING-SERVICE:" + "testing service");
        return "v.1.0.0";
    }

    @PostMapping()
    public String PublishRating(@Validated @RequestBody PublishRatingCommand command) {
        logger.info("COMMAND-RATING-SERVICE:" + "published rating command = " + command.toString());
        return command.toString();
    }

    @PutMapping() 
    public String UpdateRating(@Validated @RequestBody UpdateRatingCommand command) {
        logger.info("COMMAND-RATING-SERVICE:" + "updated rating command = " + command.toString());
        return command.toString();
    }

    @DeleteMapping() 
    public String DeleteRating(@Validated @RequestBody DeleteRatingCommand command) {
        logger.info("COMMAND-RATING-SERVICE:" + "deleted rating command = " + command.toString());
        return command.toString();
    }
}
