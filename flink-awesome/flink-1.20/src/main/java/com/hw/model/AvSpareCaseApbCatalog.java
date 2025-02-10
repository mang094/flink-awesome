package com.hw.model;

import lombok.Data;
import java.time.LocalDateTime;

@Data
public class AvSpareCaseApbCatalog {
    private Long catalogId;
    private Long apbBrandId;
    private Long apbCatalogImageId;
    private String brandName;
    private String catalogName;
    private Integer type;
    private Long parentCatalogId;
    private String parentCatalogName;
    private Integer catalogLevel;
    private Integer lastType;
    private Integer sourceId;
    private LocalDateTime createdTime;
    private LocalDateTime updatedTime;
} 