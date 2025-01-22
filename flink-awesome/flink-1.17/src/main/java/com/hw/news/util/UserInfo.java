package com.hw.news.util;

import lombok.Data;

import java.util.Date;

@Data
public class UserInfo {
    private Long id;
    private String name;
    private Integer age;
    private String email;
    private Date updateTime;
} 