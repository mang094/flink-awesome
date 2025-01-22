package com.hw.news.task.wechat.function;

import com.hsmap.news.task.wechat.dto.WechatNewsDto;
import com.hsmap.news.util.DateUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;
import java.util.UUID;

import static com.hsmap.news.util.JsonTextUtil.parseTextValue;

public class WechatMapFunction implements MapFunction<String, WechatNewsDto> {
    private final ObjectMapper objectMapper = new ObjectMapper();
    @Override
    public WechatNewsDto map(String message) throws Exception {
        Map<String, Object> dataMap = objectMapper.readValue(message, Map.class);
        WechatNewsDto newsDto = new WechatNewsDto();
        newsDto.setUuid(UUID.randomUUID().toString());
        newsDto.setType("wechat");
        newsDto.setHeadline(parseTextValue(dataMap, "head_line"));
        newsDto.setPublishTime(DateUtil.publishTime((String) dataMap.get("publish_time")));
        newsDto.setPublishSource(parseTextValue(dataMap, "publish_source"));
        newsDto.setContent(parseTextValue(dataMap, "content"));
        newsDto.setPicture(parseTextValue(dataMap, "cover_pic"));
        newsDto.setDigest(parseTextValue(dataMap, "digest"));
        newsDto.setUrl(parseTextValue(dataMap, "url"));
        newsDto.setCompanyName(parseTextValue(dataMap, "company_name"));
        newsDto.setCompanyCode(parseTextValue(dataMap, "company_code"));
        return newsDto;
    }
}
