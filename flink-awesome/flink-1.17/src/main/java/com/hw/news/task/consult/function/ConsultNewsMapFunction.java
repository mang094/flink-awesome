package com.hw.news.task.consult.function;

import com.hw.news.task.consult.dto.ConsultNewsDto;
import com.hw.news.util.DateUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;
import java.util.UUID;

import static com.hw.news.util.JsonTextUtil.*;

public class ConsultNewsMapFunction implements MapFunction<String, ConsultNewsDto> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public ConsultNewsDto map(String message) throws Exception {
        Map<String, Object> dataMap = objectMapper.readValue(message, Map.class);
        ConsultNewsDto dto = new ConsultNewsDto();
        dto.setUuid(UUID.randomUUID().toString());
        dto.setType("consult");
        dto.setHeadline(parseTextValue(dataMap.get("headline")));
        dto.setContent(parseTextValue(dataMap.get("content")));
        dto.setDigest(parseTextValue(dataMap.get("digest")));
        dto.setIndustryLabel(parseTextValue(dataMap.get("ind_tag")));
        dto.setCoverPic(parseTextValue(dataMap.get("cover_pic")));
        dto.setUrl(parseTextValue(dataMap.get("url")));
        dto.setPublishTime(DateUtil.publishTime((String) dataMap.get("publish_time")));
        dto.setPublishSite(parseTextValue(dataMap.get("publish_site")));
        return dto;
    }
}
