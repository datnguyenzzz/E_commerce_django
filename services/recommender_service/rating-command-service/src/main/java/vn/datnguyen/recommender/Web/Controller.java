package vn.datnguyen.recommender.Web;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v/1.0.0")
public class Controller {
    
    @RequestMapping(method = RequestMethod.GET)
    public String version() {
        return "v.1.0.0";
    }
}
