package com.kafka.provider.entity;

import jakarta.persistence.*;
import lombok.*;

@NoArgsConstructor
@Getter
@Setter
@Entity
public class Log {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_time")
    private String eventTime;

    @Column(name = "event_type")
    private String eventType;

    @Column(name = "product_id")
    private String productId;

    @Column(name = "catrgory_id")
    private String categoryId;

    @Column(name = "catrgory_code")
    private String categoryCode;

    @Column(name = "brand")
    private String brand;

    @Column(name = "price")
    private String price;

    @Column(name = "user_id")
    private String userId;

    @Column(name = "user_session")
    private String userSession;

    @Builder
    public Log(String eventTime, String eventType, String productId, String categoryId, String categoryCode, String brand, String price, String userId, String userSession) {
        this.eventTime = eventTime;
        this.eventType = eventType;
        this.productId = productId;
        this.categoryId = categoryId;
        this.categoryCode = categoryCode;
        this.brand = brand;
        this.price = price;
        this.userId = userId;
        this.userSession = userSession;
    }

}
