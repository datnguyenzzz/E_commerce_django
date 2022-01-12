package vn.datnguyen.recommender.Web;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import vn.datnguyen.recommender.Backend.PublishRatingCommand;

@RestController
@RequestMapping("/api/v/1.0.0")
public class Controller {
    
    @PostMapping("/rating")
    public String publishRating(@RequestBody PublishRatingCommand command) {
        return command.toString();
    }

}
