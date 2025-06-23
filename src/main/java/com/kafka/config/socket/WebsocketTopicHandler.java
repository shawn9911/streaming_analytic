package com.kafka.config.socket;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

@RequiredArgsConstructor
@Component
public class WebsocketTopicHandler {

    private final SimpMessagingTemplate messagingTemplate;

    // kafka의 user-activity-topic에 해당하는 값을 가져와서 socket에 보내기

    // 1. 실시간 인기 카테고리(view)
    @KafkaListener(topics = "result-category-views")
    public void categoryViewsHandler(String jsonData) {
        messagingTemplate.convertAndSend("/topic/category_views", jsonData);
        System.out.println("Sent data with WebSocket: " + jsonData);
    }

    // 2. 실시간 인기 브랜드(view)
    @KafkaListener(topics = "result-brand-views")
    public void brandViewsHandler(String jsonData) {
        messagingTemplate.convertAndSend("/topic/brand_views", jsonData);
        System.out.println("Sent data with WebSocket: " + jsonData);
    }

    // 3. 시간대별 사용자 활동량 분석(피크 시간 확인)
    @KafkaListener(topics = "result-hourly-users")
    public void hourlyUsersHandler(String jsonData) {
        messagingTemplate.convertAndSend("/topic/hourly_users", jsonData);
        System.out.println("Sent data with WebSocket: " + jsonData);
    }

    // 4. 사용자 구매 빈도 분포
    @KafkaListener(topics = "result-purchase-frequency")
    public void purchaseFrequencyHandler(String jsonData) {
        messagingTemplate.convertAndSend("/topic/purchase_frequency", jsonData);
        System.out.println("Sent data with WebSocket: " + jsonData);
    }

//    // 5. 사용자 구매 금액 백분위
//    @KafkaListener(topics = "result-decile")
//    public void decileHandler(String jsonData) {
//        messagingTemplate.convertAndSend("/topic/decile", jsonData);
//        System.out.println("Sent data with WebSocket: " + jsonData);
//    }
//
//    // 6. 할인율에 따른 구매율 증가
//    @KafkaListener(topics = "result-purchase-discount")
//    public void purchaseDiscountHandler(String jsonData) {
//        messagingTemplate.convertAndSend("/topic/purchase_discount", jsonData);
//        System.out.println("Sent data with WebSocket: " + jsonData);
//    }

}