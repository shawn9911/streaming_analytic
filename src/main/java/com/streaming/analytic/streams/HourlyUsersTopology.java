package com.streaming.analytic.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.streaming.analytic.dto.Event;
import com.streaming.analytic.dto.ViewsResult;
import com.streaming.analytic.dto.HourlyCount;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.*;
import java.util.stream.Collectors;

@Configuration
@EnableKafkaStreams
public class HourlyUsersTopology {
    @Bean(name = "hourly_users")
    public Topology hourlyUsersTopology(StreamsBuilder builder) {
        ObjectMapper om = new ObjectMapper();
        om.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
        om.registerModule(new JavaTimeModule());

        JsonSerde<Event> eventSerde = new JsonSerde<>(Event.class, om);

        // 1. 이벤트 스트림
        KStream<String, Event> events = builder.stream("log-data",
                        Consumed.with(Serdes.String(), eventSerde)
                                .withTimestampExtractor((record, partitionTime) -> ((Event)record.value()).eventTime().toEpochMilli()))
                .filter((key, value) -> value != null && value.eventTime() != null && value.userId() != null);

        // 2. 시간별로 userId Set 집계
        KTable<String, Set<String>> hourlyUsers = events
                .map((k, v) -> {
                    String hour = String.format("%02d", v.eventTime().atZone(java.time.ZoneId.of("UTC")).getHour());
                    return KeyValue.pair(hour, v.userId());
                })
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .aggregate(
                        HashSet::new,
                        (hour, userId, set) -> { set.add(userId); return set; },
                        Materialized.with(Serdes.String(), new JsonSerde<>(new TypeReference<Set<String>>(){}))
                );

        // 3. all_hours 키로 매핑 → 하나의 Map에 누적
        hourlyUsers.toStream()
                .map((hour, userSet) ->
                        KeyValue.pair(
                                "all_hours",
                                new HourlyCount(userSet.size(), hour)
                        )
                )
                .groupByKey(Grouped.with(
                        Serdes.String(),
                        new JsonSerde<>(HourlyCount.class, om)
                ))
                .aggregate(
                        HashMap::new,
                        (aggKey, hc, aggMap) -> {
                            aggMap.put(hc.getHour(), hc.getCount());
                            return aggMap;
                        },
                        Materialized.with(
                                Serdes.String(),
                                new JsonSerde<>(new TypeReference<HashMap<String, Integer>>(){} , om)
                        )
                )
                .toStream()
                .mapValues(hourlyCountMap -> toHourlyUsersJson(hourlyCountMap, om))
                .to("result-hourly-users", Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }

    // JSON 변환 함수
    private String toHourlyUsersJson(Map<String, Integer> hourlyCountMap, ObjectMapper om) {
        List<String> labels = new ArrayList<>();
        List<Integer> data = new ArrayList<>();
        for(int i = 0; i < 24; i++) {
            String h = String.format("%02d", i);
            labels.add(h);
            data.add(hourlyCountMap.getOrDefault(h, 0));
        }
        ViewsResult result = new ViewsResult("hourly_users", "Hourly Active Users", labels,
                data.stream().map(Long::valueOf).collect(Collectors.toList()));
        try {
            return om.writeValueAsString(result);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
