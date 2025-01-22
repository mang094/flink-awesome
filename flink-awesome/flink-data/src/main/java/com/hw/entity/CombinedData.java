package com.hw.entity;

import java.sql.Date;

public class CombinedData {
    private Long id;
    private String mainData;
    private String subData;
    private Date timestamp;
    
    // Getters and Setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public String getMainData() { return mainData; }
    public void setMainData(String mainData) { this.mainData = mainData; }
    public String getSubData() { return subData; }
    public void setSubData(String subData) { this.subData = subData; }
    public Date getTimestamp() { return timestamp; }
    public void setTimestamp(Date timestamp) { this.timestamp = timestamp; }
}