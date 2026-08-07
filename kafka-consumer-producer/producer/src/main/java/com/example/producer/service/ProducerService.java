package com.example.producer.service;
import com.example.producer.dtos.MemberDto;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional
public class ProducerService {
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public ProducerService(KafkaTemplate<String, Object> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }
    public void kafkaMessageCreate(MemberDto dto) {
        try {
            System.out.println(dto);
            String memberData = objectMapper.writeValueAsString(dto);
            kafkaTemplate.send("member-topic", memberData);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

}
