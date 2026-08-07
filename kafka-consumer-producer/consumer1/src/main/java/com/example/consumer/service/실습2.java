//package com.example.consumer.service;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import org.springframework.kafka.annotation.KafkaListener;
//import org.springframework.stereotype.Service;
//import org.springframework.transaction.annotation.Transactional;
//
//@Service
//@Transactional
//public class 실습2 {
//    private final ObjectMapper objectMapper;
//
//    public 실습2(ObjectMapper objectMapper) {
//        this.objectMapper = objectMapper;
//    }
//
////  테스트1 : 2대의 컨슈머서버가 같은 그룹ID를 가지고 같은 topic을 listen 경우
////  테스트2 : 2대의 컨슈머서버가 다른 그룹ID를 가지고 같은 topic을 listen 경우
//    @KafkaListener(
//            topics = "member-topic",
//            groupId = "${spring.kafka.consumer.member-topic-log-group-id}",
//            containerFactory = "kafkaListener"
//    )
//    public void consumer1(String message) {
//        System.out.println("member-topic-log-group 메시지 수신 : " + message);
//    }
//}
