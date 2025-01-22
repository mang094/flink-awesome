package com.hw.news.task.realtime.function;

import com.hsmap.news.task.realtime.dto.RealtimeNewsDto;
import com.hsmap.news.util.DateUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class NewsMapFunction implements MapFunction<String, RealtimeNewsDto> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public RealtimeNewsDto map(String message) throws Exception {
        Map objectMap = objectMapper.readValue(message, Map.class);
        Map<String, Object> dataMap = (Map<String, Object>) objectMap.get("data");
        Date now = new Date();
        System.out.println(now + " - This record is match successfully.");
        System.out.println(now + " - " + dataMap.get("title"));
        RealtimeNewsDto dto = new RealtimeNewsDto();
        dto.setUuid(UUID.randomUUID().toString());
        dto.setType("realtime");
        dto.setTitle((String) dataMap.get("title"));
        dto.setContent(content(dataMap));
        dto.setDetailUrl((String) dataMap.get("detail_url"));
        dto.setSiteName((String) dataMap.get("site_name"));
        dto.setPublishTime(DateUtil.publishTime((String) dataMap.get("publish_time")));
        return dto;
    }

    /**
     * double confirmed with Sihao, it should be string type, she will help handle the source data in kafka
     *
     * @param dataMap
     * @return
     */
    private String content(Map<String, Object> dataMap) {
        String content ;
        if(dataMap.get("content") instanceof List) {
            List<String> contentList = (List<String>) dataMap.get("content");
            StringBuilder stringBuilder = new StringBuilder();
            for (String ctValue : contentList) {
                stringBuilder.append(ctValue);
            }
            content = stringBuilder.toString();
        } else {
            content = (String) dataMap.get("content");
        }
        return content;
    }
}
