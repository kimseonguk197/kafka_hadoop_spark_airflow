package com.example.consumer.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

@Service
@Transactional
public class 실습1 {
    private final JsonMapper jsonMapper;

    public 실습1(JsonMapper jsonMapper) {
        this.jsonMapper = jsonMapper;
    }

//    테스트1. 메시지 송수신 기본 실습(하나의 컨슈머그룹에 하나의 컨슈머가 1개의 토픽을 listen)
//    테스트2. 컨슈머 서버 down → 메시지 발행 → 재시작 이후 메시지 수신 여부 확인
    @KafkaListener(
            topics = "member-topic",
            groupId = "${spring.kafka.consumer.member-topic-log-group-id}", // yml에서 불러오기
            containerFactory = "kafkaListener"
    )
    public void consumer1(String message) {
        System.out.println("member-topic-log-group 메시지 수신 : " + message);
    }

}