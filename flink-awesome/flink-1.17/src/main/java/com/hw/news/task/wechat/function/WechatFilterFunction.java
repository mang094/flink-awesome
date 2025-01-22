package com.hw.news.task.wechat.function;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

import static com.hw.news.util.JsonTextUtil.parseTextValue;

public class WechatFilterFunction implements FilterFunction<String> {
    private final ObjectMapper objectMapper = new ObjectMapper();
    @Override
    public boolean filter(String message) throws Exception {
        Map<String, Object> dataMap = objectMapper.readValue(message, Map.class);
        String source = parseTextValue(dataMap, "publish_source");
        return "官网".equals(source) || "官微".equals(source);
    }
}
