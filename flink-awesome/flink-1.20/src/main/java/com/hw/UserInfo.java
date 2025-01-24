package com.hw;

import lombok.Data;

import java.sql.Timestamp;

@Data
public class UserInfo {
    private Long id;
    private String name;
    private Integer age;
    private String email;
    private Timestamp updateTime;
} 