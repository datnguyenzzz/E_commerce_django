package vn.datnguyen.recommender.Web;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v/1.0.0/rating")
public class ApiController {
    
    @GetMapping("/test")
    public String testApi() {
        return "v.1.0.0";
    }
}
