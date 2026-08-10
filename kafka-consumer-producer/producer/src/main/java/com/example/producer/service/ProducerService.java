package com.example.producer.service;
import com.example.producer.dtos.MemberDto;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

@Service
@Transactional
public class ProducerService {
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final JsonMapper jsonMapper;

    public ProducerService(KafkaTemplate<String, Object> kafkaTemplate, JsonMapper jsonMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.jsonMapper = jsonMapper;
    }
    public void kafkaMessageCreate(MemberDto dto) {
        System.out.println(dto);
        String memberData = jsonMapper.writeValueAsString(dto);
        kafkaTemplate.send("member-topic", memberData);
    }

}
