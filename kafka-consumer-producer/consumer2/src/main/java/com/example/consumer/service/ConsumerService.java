package com.example.consumer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional
public class ConsumerService {
    private final ObjectMapper objectMapper;
    public ConsumerService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

//  테스트1 : 2대의 컨슈머서버가 같은 그룹ID를 가지고 같은 topic을 listen 경우
//  테스트2 : 2대의 서버가 같은 그룹ID를 가지고 같은 topic을 listen 경우
//  테스트3 : offset earliest를 통한 과거 메시지 read 확인
    @KafkaListener(
            topics = "member-topic",
            groupId = "${spring.kafka.consumer.member-topic-log-group-id}",
//            groupId = "${spring.kafka.consumer.member-topic-static-group-id}",
            containerFactory = "kafkaListener"
    )
    public void consumer1(String message) {
        System.out.println("member-topic 메시지 수신 : " + message);
    }
}
