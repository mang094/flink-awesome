package com.hw.news.task.consult.dto;

import lombok.Data;

import java.io.Serializable;
import java.sql.Timestamp;

@Data
public class ConsultNewsDto implements Serializable {
    private String uuid;
    private String type;
    private String headline;
    private String content;
    private String digest;
    private String industryLabel;
    private String coverPic;
    private String url;
    private String publishSite;
    private Timestamp publishTime;
}
