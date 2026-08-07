package com.example.producer.dtos;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;


@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class MemberDto {
    private String name;
    private String email;
    private int age;
    @Builder.Default
    private LocalDateTime createdAt = LocalDateTime.now();
}