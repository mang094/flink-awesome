package com.hw.news.task.realtimenew.function;

import com.hw.news.util.DateUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Date;
import java.util.Map;

/**
 * filter - only accept publish time within 2 days.
 */
@Slf4j
public class NewsFilterFunction implements FilterFunction<String> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public boolean filter(String value) throws Exception {
        // negative case 1.
        Map objectMap = objectMapper.readValue(value, Map.class);
        if (objectMap == null) {
            return false;
        }
        Map<String, Object> dataMap = (Map<String, Object>) objectMap.get("data");
        if (dataMap == null) {
            return false;
        }
        String publishTimeDesc = (String) dataMap.get("publish_time");
        if (StringUtils.isBlank(publishTimeDesc)) {
            return false;
        }
        Date publishTime = formatPublishTime(publishTimeDesc);
        if (publishTime == null) {
            return false;
        }
        // data within 2 days are accepted.
        //return publishTime.after(DateUtil.subtractDays(new Date(), 2));
        //只需要今天的数据
        return DateUtil.isToday(publishTime);
    }

    private Date formatPublishTime(String publishTimeDesc) {
        Date publishTime = null;
        try {
            // yyyy-MM-dd hh:mm:ss
            publishTime = DateUtil.getStrToDateFormat(publishTimeDesc, DateUtil.DATE_FORMAT_1);
            // yyyy-MM-dd hh:mm
            if (publishTime == null) {
                publishTime = DateUtil.getStrToDateFormat(publishTimeDesc, DateUtil.DATE_FORMAT_2);
            }
            // yyyy-MM-dd
            if(publishTime == null) {
                publishTime = DateUtil.getStrToDateFormat(publishTimeDesc, DateUtil.DATE_FORMAT_3);
            }
            return publishTime;
        } catch (Exception e) {
            log.error("时间解析异常:{}",e);
        }
        return publishTime;
    }
}
