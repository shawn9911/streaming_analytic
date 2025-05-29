package com.streaming.analytic.dto;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;
import java.time.Instant;

public class InstantUtcDeserializer extends JsonDeserializer<Instant> {

    @Override
    public Instant deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        String raw = p.getText().trim();

        try {
            // ISO-8601 포맷
            return Instant.parse(raw.replace(" UTC", "Z").replace(" ", "T"));
        } catch (Exception ignored) {}

        try {
            // 에폭 타임스탬프 (초 또는 나노 포함)
            return Instant.ofEpochSecond((long) Double.parseDouble(raw));
        } catch (Exception ignored) {}

        throw new IllegalArgumentException("Cannot parse Instant from: " + raw);
    }
}
