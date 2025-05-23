package com.streaming.analytic.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streaming.analytic.dto.Event;
import com.streaming.analytic.dto.ViewsResult;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Configuration
@EnableKafkaStreams
public class ViewsTopology {

    private static final Duration WINDOW_SIZE = Duration.ofHours(1);
    private static final int TOP_K = 10;

    @Bean(name = "category_views")
    public Topology categoryViewsTopology(StreamsBuilder builder) {
        // category_code 기준
        return buildViewsTopology(
                builder,
                "category_views", // 결과 토픽명
                "category_code",  // 집계 기준 필드명
                "category_views", // type
                "Category View Per Hour" // label
        );
    }

    @Bean(name = "brand_views")
    public Topology brandViewsTopology(StreamsBuilder builder) {
        // brand 기준
        return buildViewsTopology(
                builder,
                "brand_views",    // 결과 토픽명
                "brand",          // 집계 기준 필드명
                "brand_views",    // type
                "Brand View Per Hour" // label
        );
    }

    public static Topology buildViewsTopology(
            StreamsBuilder builder,
            String outputTopic,
            String fieldName,
            String type,
            String labelName
    ) {
        JsonSerde<Event> eventSerde = new JsonSerde<>(Event.class);
        ObjectMapper om = new ObjectMapper();

        // 기준 필드 선택자 함수
        Function<Event, String> keySelector = e -> {
            switch (fieldName) {
                case "category_code": return e.category_code();
                case "brand": return e.brand();
                default: throw new IllegalArgumentException("Unknown field: " + fieldName);
            }
        };

        KStream<String, Event> views = builder.stream("log-data",
                        Consumed.with(Serdes.String(), eventSerde)
                                .withTimestampExtractor((record, partitionTime) ->
                                        ((Event) record.value()).event_time().toEpochMilli()))
                .filter((key, value) -> value != null
                        && "view".equals(value.event_type())
                        && keySelector.apply(value) != null);

        KTable<Windowed<String>, Long> counts = views
                .groupBy((key, value) -> keySelector.apply(value),
                        Grouped.with(Serdes.String(), eventSerde))
                .windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(WINDOW_SIZE))
                .count();

        counts.toStream()
                .map((windowedKey, count) -> KeyValue.pair(
                        windowedKey.window().start(),
                        new AbstractMap.SimpleEntry<>(windowedKey.key(), count)
                ))
                .groupByKey(Grouped.with(Serdes.Long(), new JsonSerde<>(new TypeReference<AbstractMap.SimpleEntry<String, Long>>() {})))
                .aggregate(
                        HashMap::new,
                        (windowStart, entry, aggregate) -> {
                            aggregate.put(entry.getKey(), entry.getValue());
                            return aggregate;
                        },
                        Materialized.with(Serdes.Long(), new JsonSerde<>(new TypeReference<HashMap<String, Long>>() {}))
                )
                .toStream()
                .mapValues(countMap -> toViewsJson(type, labelName, countMap, om))
                .to(outputTopic, Produced.with(Serdes.Long(), Serdes.String()));

        return builder.build();
    }

    private static String toViewsJson(String type, String labelName, Map<String, Long> countMap, ObjectMapper om) {
        List<Map.Entry<String, Long>> topList = countMap.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                .limit(TOP_K)
                .collect(Collectors.toList());

        List<String> labels = topList.stream().map(Map.Entry::getKey).collect(Collectors.toList());
        List<Long> data = topList.stream().map(Map.Entry::getValue).collect(Collectors.toList());

        ViewsResult result = new ViewsResult(type, labelName, labels, data);

        try {
            return om.writeValueAsString(result);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
