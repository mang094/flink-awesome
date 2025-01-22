package com.hw.entity;

import java.sql.Date;

public class MainTableData {
    private Long id;
    private String mainData;
    private Date timestamp;
    
    // Getters and Setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public String getMainData() { return mainData; }
    public void setMainData(String mainData) { this.mainData = mainData; }
    public Date getTimestamp() { return timestamp; }
    public void setTimestamp(Date timestamp) { this.timestamp = timestamp; }
}