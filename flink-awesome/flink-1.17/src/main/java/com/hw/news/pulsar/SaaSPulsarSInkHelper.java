package com.hw.news.pulsar;


import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.pulsar.sink.PulsarSink;


/**
 * @ClassName SaaSPulsarSInkHelper
 * @Description
 * @Author zh
 * @Date 2023/12/28 14:48
 * @Version
 */
public class SaaSPulsarSInkHelper {
    private SaaSPulsarSInkHelper() {}
    public static final String BOOTSTRAP_SERVERS = "192.168.200.101:9092,192.168.200.102:9092,192.168.200.103:9092";
    public static final String TOPIC_NEWS_FIRST = "news-staging";
    public static final String TRANSACTION_PREFIX = "firestone-saas-";
    public static final String TRANSACTION_TIMEOUT = 10 * 60 * 1000 + ""; // 10 minutes
    public static PulsarSink<String> pulsarSink() {


        return PulsarSink.builder()
               .setServiceUrl(BOOTSTRAP_SERVERS)
               .setAdminUrl("http://192.168.200.101:8080")
               .setTopics("news-staging")
               .setSerializationSchema(new SimpleStringSchema())
               .build();

    }


}

