package com.streaming.analytic.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streaming.analytic.dto.Event;
import com.streaming.analytic.dto.ViewsResult;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.*;
import java.util.stream.Collectors;

public class PurchaseFrequencyTopology {
    @Bean(name = "purchase_frequency")
    public Topology purchaseFrequencyTopology(StreamsBuilder builder) {
        JsonSerde<Event> eventSerde = new JsonSerde<>(Event.class);
        ObjectMapper om = new ObjectMapper();

        // 1. purchase 이벤트 필터링
        KStream<String, Event> purchases = builder.stream("log-data",
                        Consumed.with(Serdes.String(), eventSerde)
                                .withTimestampExtractor((record, partitionTime) -> ((Event)record.value()).event_time().toEpochMilli()))
                .filter((key, value) -> value != null
                        && "purchase".equals(value.event_type())
                        && value.user_id() != null);

        // 2. user_id별로 구매 건수 집계
        KTable<String, Long> userPurchaseCounts = purchases
                .groupBy((key, value) -> value.user_id(), Grouped.with(Serdes.String(), eventSerde))
                .count();

        // 3. 구매 횟수 구간별로 사용자 수 집계
        userPurchaseCounts
                .toStream()
                .map((userId, count) -> KeyValue.pair(purchaseRange(count), 1L))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                .reduce(Long::sum)
                .toStream()
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                .aggregate(
                        () -> new HashMap<String, Long>(),
                        (range, cnt, agg) -> { agg.put(range, cnt); return agg; },
                        Materialized.with(Serdes.String(), new JsonSerde<>(Map.class))
                )
                .toStream()
                .mapValues(freqMap -> toPurchaseFrequencyJson(freqMap, om))
                .to("purchase_frequency", Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }

    // 구매 횟수 구간화 함수
    private static String purchaseRange(long count) {
        if (count == 1) return "1회";
        if (count <= 5) return "2-5회";
        if (count <= 10) return "6-10회";
        return "10회 이상";
    }

    // JSON 변환 함수
    private String toPurchaseFrequencyJson(Map<String, Long> freqMap, ObjectMapper om) {
        List<String> labels = List.of("1회", "2-5회", "6-10회", "10회 이상");
        List<Long> data = labels.stream().map(lab -> freqMap.getOrDefault(lab, 0L)).collect(Collectors.toList());
        ViewsResult result = new ViewsResult("purchase_frequency", "User Purchase Frequency", labels, data);
        try {
            return om.writeValueAsString(result);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
