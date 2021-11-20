package com.example.GatheringService.web;

import com.example.GatheringService.dto.GatherRequest;

import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import org.slf4j.Logger;

@RestController
@RequestMapping(value = "/api/v1.0",
                produces = "application/json")
public class ApiController {

    private static final Logger logger = LoggerFactory.getLogger(ApiController.class);
    
    @GetMapping("/version")
    public String getVersion() {
        return "v1.0";
    }

    @PostMapping(value = "/gather",
                 produces = "application/json")
    @ResponseStatus(HttpStatus.CREATED)
    public String handlePostRequest(@RequestBody GatherRequest request) {
        logger.info("Gathering request: " + request.toString());
        return request.toString();
    }
}
