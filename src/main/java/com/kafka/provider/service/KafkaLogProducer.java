package com.kafka.provider.service;

import com.kafka.provider.entity.Log;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaLogProducer {

    @Autowired
    private final KafkaTemplate<String, Log> kafkaTemplate;

    public KafkaLogProducer(KafkaTemplate<String, Log> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendToKafka(String topic, Log log) {
        //System.out.println("sendToKafka");
        kafkaTemplate.send(topic, log);
    }

}
