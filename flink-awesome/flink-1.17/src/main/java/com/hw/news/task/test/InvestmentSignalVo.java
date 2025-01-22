package com.hw.news.task.test;


import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import org.apache.flink.types.Row;

@Data
public class InvestmentSignalVo {

    @ApiModelProperty("企业编码")
    private String companyCode;

    public InvestmentSignalVo(Row row) {
        this.companyCode = (String)row.getField(0); // 假设第一个字段是 id

    }

}
