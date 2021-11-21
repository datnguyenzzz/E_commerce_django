package com.example.GatheringService.dto;

import javax.validation.constraints.NotNull;

import lombok.Data;
import lombok.ToString;

@ToString
@Data
public class GatherRequest {
    @NotNull
    private String word;

    @NotNull
    private String lang;

    public String getWord() {
        return word;
    }

    public String getLang() {
        return lang;
    }

}
