package com.hw.entity;

import java.sql.Date;

public class SubTableData {

    private Long id;
    private Long mainTableId;
    private String subData;
    private Date timestamp;
    
    // Getters and Setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public Long getMainTableId() { return mainTableId; }
    public void setMainTableId(Long mainTableId) { this.mainTableId = mainTableId; }
    public String getSubData() { return subData; }
    public void setSubData(String subData) { this.subData = subData; }
    public Date getTimestamp() { return timestamp; }
    public void setTimestamp(Date timestamp) { this.timestamp = timestamp; }
}