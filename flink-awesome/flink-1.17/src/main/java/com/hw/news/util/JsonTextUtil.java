package com.hw.news.util;

import java.util.Map;

public final class JsonTextUtil {

    private JsonTextUtil() {

    }

    public static String parseTextValue(Object columnValue) {
        return columnValue != null ? (String) columnValue : "";
    }

    public static String parseTextValue(Map<String, Object> dataMap, String columnName) {
        return parseTextValue(dataMap.get(columnName));
    }
}
