package com.example.GatheringService.dto;

import lombok.Data;
import lombok.NonNull;
import lombok.ToString;

@ToString
@Data
public class GatherRequest {
    @NonNull
    private String word;

    @NonNull
    private String lang;
}
