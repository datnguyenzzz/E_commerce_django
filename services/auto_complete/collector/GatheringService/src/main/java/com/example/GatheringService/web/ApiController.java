package com.example.GatheringService.web;

import com.example.GatheringService.dto.GatherRequest;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/api/v1.0",
                produces = "application/json")
public class ApiController {
    
    @GetMapping("/version")
    public String getVersion() {
        return "v1.0";
    }

    @PostMapping(value = "/gather",
                 produces = "application/json")
    public String handlePostRequest(@RequestBody GatherRequest request) {
        return request.toString();
    }
}
