package com.hw.entity;

import java.io.Serializable;
import java.sql.Date;

public class CombinedData implements Serializable {
    private Long id;
    private String mainData;
    private String subData;
    private Date timestamp;

    public CombinedData() {

    }

    // Getters and Setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public String getMainData() { return mainData; }
    public void setMainData(String mainData) { this.mainData = mainData; }
    public String getSubData() { return subData; }
    public void setSubData(String subData) { this.subData = subData; }
    public Date getTimestamp() { return timestamp; }
    public void setTimestamp(Date timestamp) { this.timestamp = timestamp; }


    public CombinedData(MainTableData mainData, String subDataList) {

        this.id = mainData.getId();

        this.timestamp = mainData.getTimestamp();

        this.mainData = mainData.getMainData();

        this.subData = subDataList;

    }

}