package com.hw.news.task.blog.dto;

import lombok.Data;

import java.sql.Timestamp;

@Data
public class BlogNewsDto {
    private String uuid;
    private String type;
    private String profileName;
    private String profilePhoto;
    private Timestamp publishTime;
    private String publishSource;
    private String content ;
    private String picture;
    private String digest;
    private String url;
    private String companyName;
    private String companyCode;


}









