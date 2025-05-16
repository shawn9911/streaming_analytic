package com.kafka.provider.repository;

import com.kafka.provider.entity.Log;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface LogRepository extends JpaRepository<Log, Long> {
    List<Log> findTop100ByOrderByEventTimeAsc();
}
