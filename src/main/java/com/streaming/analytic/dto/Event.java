package com.streaming.analytic.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

public record Event(String category_code,
                    String event_type,
                    String brand,
                    Instant event_time,
                    String user_id,
                    double price) {

    @JsonCreator
    public Event(@JsonProperty("category_code") String category_code,
                 @JsonProperty("event_type") String event_type,
                 @JsonProperty("brand") String brand,
                 @JsonProperty("event_time") String event_time,
                 @JsonProperty("user_id") String user_id,
                 @JsonProperty("price") double price) {
        this(category_code, event_type, brand, parseTime(event_time), user_id, price);
    }

    private static Instant parseTime(String raw) {
        String iso = raw.replace(" UTC", "Z").replace(" ", "T");
        return Instant.parse(iso);
    }
}
