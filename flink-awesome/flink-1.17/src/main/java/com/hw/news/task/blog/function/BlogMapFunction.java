package com.hw.news.task.blog.function;

import com.hsmap.news.task.blog.dto.BlogNewsDto;
import com.hsmap.news.util.DateUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;
import java.util.UUID;

import static com.hsmap.news.util.JsonTextUtil.parseTextValue;

public class BlogMapFunction implements MapFunction<String, BlogNewsDto> {
    private final ObjectMapper objectMapper = new ObjectMapper();
    @Override
    public BlogNewsDto map(String message) throws Exception {
        System.out.println("message: " + message);
        Map<String, Object> dataMap = objectMapper.readValue(message, Map.class);
        BlogNewsDto newsDto = new BlogNewsDto();
        newsDto.setUuid(UUID.randomUUID().toString());
        newsDto.setType("blog");
        newsDto.setProfileName(parseTextValue(dataMap, "profile_name"));
        newsDto.setProfilePhoto(parseTextValue(dataMap, "profile_photo"));
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
