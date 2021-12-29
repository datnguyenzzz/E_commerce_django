package com.example.GatheringService.dto;


import lombok.Data;
import lombok.ToString;

@ToString
@Data
public class GatherRequest {
    private String word;

    private String lang;

    public String getWord() {
        return word;
    }

    public String getLang() {
        return lang;
    }

}
