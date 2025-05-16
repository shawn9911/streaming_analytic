package com.kafka.provider.service;

import com.kafka.provider.entity.Log;
import com.kafka.provider.repository.LogRepository;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;

@RequiredArgsConstructor
@Service
public class KafkaScheduler {

    private final LogRepository logRepository;
    private final KafkaLogProducer producer;

    private boolean fileLoadFinish = false;

    /*
        보내고 지우는 방식으로 중복제거
     */
    @Transactional
    @Scheduled(fixedRate = 5000)
    public void sendToKafkaAndDelete() {
        if (fileLoadFinish) {
            List<Log> logs = logRepository.findTop100ByOrderByEventTimeAsc();

            for (Log log : logs) {
                producer.sendToKafka("log-data", log);
                logRepository.delete(log);
            }
        }
    }

    public void activate() {
        this.fileLoadFinish = true;
    }

}
