package vn.datnguyen.recommender.Web;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class Controller {
    @GetMapping("/api/version")
    public String version() {
        return "v.1.0.0";
    }
}
