package com.streaming.analytic.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
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

public class HourlyUsersTopology {
    @Bean(name = "hourly_users")
    public Topology hourlyUsersTopology(StreamsBuilder builder) {
        JsonSerde<Event> eventSerde = new JsonSerde<>(Event.class);
        ObjectMapper om = new ObjectMapper();

        // 1. 시간대 추출 (0~23)
        KStream<String, Event> events = builder.stream("log-data",
                        Consumed.with(Serdes.String(), eventSerde)
                                .withTimestampExtractor((record, partitionTime) -> ((Event)record.value()).event_time().toEpochMilli()))
                .filter((key, value) -> value != null && value.event_time() != null && value.user_id() != null);

        // 2. (hour, user_id)로 그룹핑해서 user_id Set으로 집계 → Set.size()로 active user 수 뽑기
        KTable<String, Set<String>> hourlyUsers = events
                .map((k, v) -> {
                    String hour = String.format("%02d", v.event_time().atZone(java.time.ZoneId.of("UTC")).getHour());
                    return KeyValue.pair(hour, v.user_id());
                })
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .aggregate(
                        HashSet::new,
                        (hour, userId, set) -> { set.add(userId); return set; },
                        Materialized.with(Serdes.String(), new JsonSerde<>(new TypeReference<Set<String>>(){}))
                );

        // 3. 결과 변환
        hourlyUsers.toStream()
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(new TypeReference<Set<String>>(){})))
                .aggregate(
                        () -> new HashMap<String, Integer>(),
                        (hour, userSet, agg) -> { agg.put(hour, userSet.size()); return agg; },
                        Materialized.with(Serdes.String(), new JsonSerde<>(new TypeReference<HashMap<String, Integer>>(){}))
                )
                .toStream()
                .mapValues(hourlyCountMap -> toHourlyUsersJson(hourlyCountMap, om))
                .to("hourly_users", Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }

    // JSON 변환 함수
    private String toHourlyUsersJson(Map<String, Integer> hourlyCountMap, ObjectMapper om) {
        // labels: 00~23
        List<String> labels = new ArrayList<>();
        List<Integer> data = new ArrayList<>();
        for(int i = 0; i < 24; i++) {
            String h = String.format("%02d", i);
            labels.add(h);
            data.add(hourlyCountMap.getOrDefault(h, 0));
        }
        ViewsResult result = new ViewsResult("hourly_users", "Hourly Active Users", labels, data.stream().map(Long::valueOf).collect(Collectors.toList()));
        try {
            return om.writeValueAsString(result);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
