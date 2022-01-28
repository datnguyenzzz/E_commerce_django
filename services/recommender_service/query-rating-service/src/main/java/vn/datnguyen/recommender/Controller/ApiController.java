package vn.datnguyen.recommender.Controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v/1.0.0/rating")
public class ApiController {
    
    @GetMapping
    public ResponseEntity<String> getRating(@RequestParam(required = false) String clientId, @RequestParam(required = false) String itemId) {
        if (clientId != null && itemId != null) {
            return ResponseEntity.status(HttpStatus.ACCEPTED).body("HAS BOTH CLIENTID + ITEMID");
        }
        else if (clientId==null && itemId==null) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("REQUIRE AT LEAST CLIENTID OR ITEMID");
        }
        else if (clientId != null) {
            return ResponseEntity.status(HttpStatus.ACCEPTED).body("HAS ONLY CLIENTID");
        }
        else {
            return ResponseEntity.status(HttpStatus.ACCEPTED).body("HAS ONLY ITEMID");
        }
    }

}
