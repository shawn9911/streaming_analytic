package com.streaming.analytic.streams;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.streaming.analytic.dto.Event;
import com.streaming.analytic.dto.ViewsResult;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

@Configuration
@EnableKafkaStreams
public class PurchaseFrequencyTopology {

    private static final String AGGREGATE_KEY = "frequency";

    @Bean(name = "purchase_frequency")
    public Topology purchaseFrequencyTopology(StreamsBuilder builder) {
        ObjectMapper om = new ObjectMapper();
        om.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
        om.registerModule(new JavaTimeModule());

        JsonSerde<Event> eventSerde = new JsonSerde<>(Event.class, om);


        // 1. purchase 이벤트 필터링
        KStream<String, Event> purchases = builder.stream(
                        "log-data",
                        Consumed.with(Serdes.String(), eventSerde)
                                .withTimestampExtractor((record, partitionTime) -> ((Event) record.value()).eventTime().toEpochMilli())  // 변경: Object -> Event 캐스팅
                )
                .filter((key, value) -> value != null
                        && "purchase".equals(value.eventType())
                        && value.userId() != null
                );

        // 2. user_id별 구매 건수 집계
        KTable<String, Long> userPurchaseCounts = purchases
                .groupBy((key, value) -> value.userId(), Grouped.with(Serdes.String(), eventSerde))
                .count();

        // 3. user_id -> range 매핑
        KTable<String, String> userRangeTable = userPurchaseCounts
                .mapValues(PurchaseFrequencyTopology::purchaseRange,
                        Materialized.with(Serdes.String(), Serdes.String()));

        // 4. 공통 키로 단일 aggregate  (2번 적용)
        KTable<String, HashMap<String, Long>> aggregated = userRangeTable.toStream()  // 변경: 제네릭을 HashMap으로
                .map((userId, range) -> KeyValue.pair(AGGREGATE_KEY, range))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .aggregate(
                        HashMap::new,
                        (key, range, aggMap) -> {
                            aggMap.put(range, aggMap.getOrDefault(range, 0L) + 1L);
                            return aggMap;
                        },
                        Materialized.with(
                                Serdes.String(),
                                new JsonSerde<>(new TypeReference<HashMap<String, Long>>() {})
                        )
                );

        // 5. 단일 JSON 메시지 전송  (3번 적용)
        aggregated.toStream()
                .mapValues(freqMap -> toPurchaseFrequencyJson(freqMap, om))
                .to("result-purchase-frequency", Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }

    private static String purchaseRange(long count) {
        if (count == 1) return "1회";
        if (count <= 5) return "2-5회";
        if (count <= 10) return "6-10회";
        return "10회 이상";
    }

    private String toPurchaseFrequencyJson(HashMap<String, Long> freqMap, ObjectMapper om) {  // 변경: 매개변수를 HashMap으로
        List<String> labels = List.of("1회", "2-5회", "6-10회", "10회 이상");
        List<Long> data = labels.stream()
                .map(label -> freqMap.getOrDefault(label, 0L))
                .collect(Collectors.toList());
        ViewsResult result = new ViewsResult("purchase_frequency", "User Purchase Frequency", labels, data);
        try {
            return om.writeValueAsString(result);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
