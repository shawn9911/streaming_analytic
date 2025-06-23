package com.kafka.config.socket;

import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) { // 연결 요청
        registry.addEndpoint("/ws-result") // 최초 연결
                .setAllowedOriginPatterns("*");
    }

    @Override
    public void configureMessageBroker(MessageBrokerRegistry config) {
//        client가 메시지 보낼 때 사용
//        config.setApplicationDestinationPrefixes("/order"); // client -> server
        config.enableSimpleBroker("/topic","/queue"); // 구독 요청(메시지브로커를 등록함)
    }

}