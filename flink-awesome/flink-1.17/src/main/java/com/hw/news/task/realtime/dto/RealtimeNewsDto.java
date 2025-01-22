package com.hw.news.task.realtime.dto;

import lombok.Data;

import java.io.Serializable;
import java.sql.Timestamp;

@Data
public class RealtimeNewsDto implements Serializable {
    private String uuid;
    private String type;
    private String title;
    private String content;
    private String detailUrl;
    private String siteName;
    private Timestamp publishTime;

}
