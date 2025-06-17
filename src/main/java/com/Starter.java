package com;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
//@EnableScheduling  // KafkaLogProducer가 스케줄링 사용
@EnableKafkaStreams
public class Starter {
    public static void main(String[] args) {
        SpringApplication.run(Starter.class, args);
    }

}
