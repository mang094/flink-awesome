package com.hw.news.task.wechat.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.sql.Timestamp;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class WechatNewsDto implements Serializable {
    private String uuid;
    private String type;
    private String headline;
    private Timestamp publishTime;
    private String publishSource;
    private String content;
    private String picture;
    private String digest;
    private String url;
    private String companyName;
    private String companyCode;

}
