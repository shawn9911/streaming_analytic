package com.streaming.analytic.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.time.Instant;

@JsonIgnoreProperties(ignoreUnknown = true)
public record Event(
        String categoryCode,
        String eventType,
        String brand,
        @JsonDeserialize(using = InstantUtcDeserializer.class)
        Instant eventTime,
        String userId,
        double price,
        String id
) {}
