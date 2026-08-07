package com.example.producer.controller;

import com.example.producer.dtos.MemberDto;
import com.example.producer.service.ProducerService;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/producer")
public class ProducerController {
    private final ProducerService producerService;

    public ProducerController(ProducerService producerService) {
        this.producerService = producerService;
    }

    @PostMapping("/message/create")
    public void messageCreate(@RequestBody MemberDto dto){
        producerService.kafkaMessageCreate(dto);
    }


}
