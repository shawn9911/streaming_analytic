package com.streaming.analytic.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streaming.analytic.dto.Event;
import com.streaming.analytic.dto.ViewsResult;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.*;
import java.util.stream.Collectors;

public class PurchaseDecileTopology {
    @Bean(name = "decile")
    public Topology purchaseDecileTopology(StreamsBuilder builder) {
        JsonSerde<Event> eventSerde = new JsonSerde<>(Event.class);
        ObjectMapper om = new ObjectMapper();

        // 1. purchase 이벤트만 필터
        KStream<String, Event> purchases = builder.stream("log-data",
                        Consumed.with(Serdes.String(), eventSerde)
                                .withTimestampExtractor((record, partitionTime) -> ((Event)record.value()).event_time().toEpochMilli()))
                .filter((key, value) -> value != null
                        && "purchase".equals(value.event_type())
                        && value.user_id() != null
                        && value.price() > 0);

        // 2. user_id별 구매 금액 합계 집계
        KTable<String, Double> userPurchaseSum = purchases
                .groupBy((key, value) -> value.user_id(), Grouped.with(Serdes.String(), eventSerde))
                .aggregate(
                        () -> 0.0,
                        (userId, event, sum) -> sum + event.price(),
                        Materialized.with(Serdes.String(), Serdes.Double())
                );

        // 3. 모든 유저의 합계 금액 Map을 하나로 집계 (실무에서는 상태공간 문제로 주의!)
        userPurchaseSum.toStream()
                .groupBy((userId, sum) -> "all", Grouped.with(Serdes.String(), Serdes.Double()))
                .aggregate(
                        HashMap::new,
                        (key, sum, map) -> {
                            map.put(key, sum);
                            return map;
                        },
                        Materialized.with(
                                Serdes.String(),
                                new JsonSerde<>(new com.fasterxml.jackson.core.type.TypeReference<HashMap<String, Double>>() {})
                        )
                )
                .toStream()
                .filter((k, v) -> v != null && v.size() > 0)
                .mapValues(map -> toDecileJson(map, om))
                .to("decile", Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }

    // Map<String, Double>: user_id -> total_sum
    // 각 user_id의 누적금액을 받아 decile별 유저수 JSON 반환
    private String toDecileJson(Map<String, Double> purchaseMap, ObjectMapper om) {
        // 1. 누적금액으로 내림차순 정렬
        List<Double> sorted = purchaseMap.values().stream()
                .sorted(Comparator.reverseOrder())
                .collect(Collectors.toList());
        int total = sorted.size();
        // 2. 각 decile에 들어갈 개수
        int[] decileCounts = new int[10];
        for (int i = 0; i < total; i++) {
            int decile = Math.min((int) ((double)i / total * 10), 9); // 0~9
            decileCounts[decile]++;
        }
        List<String> labels = List.of(
                "P0-10", "P10-20", "P20-30", "P30-40", "P40-50",
                "P50-60", "P60-70", "P70-80", "P80-90", "P90-100"
        );
        List<Long> data = Arrays.stream(decileCounts).mapToLong(i -> (long) i).boxed().collect(Collectors.toList());

        ViewsResult result = new ViewsResult("decile", "Customer Count Per Decile", labels, data);
        try {
            return om.writeValueAsString(result);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
