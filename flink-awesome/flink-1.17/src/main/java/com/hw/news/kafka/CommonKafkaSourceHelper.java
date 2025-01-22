package com.hw.news.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

public final class CommonKafkaSourceHelper {
    public static final String BOOTSTRAP_SERVERS = "192.168.200.81:30091,192.168.200.82:30092,192.168.200.83:30093";
    public static final String TOPIC_NEWS = "news";  // 实时
    public static final String TOPIC_CONSULT_NEWS = "ads_spider_news";  // 咨询
    public static final String TOPIC_COMPANY_NEWS = "company_news"; // 官微，官网，官博
    private CommonKafkaSourceHelper() {
        // DO NOTHING
    }
    /**
     *
     * @param kafkaGroup -> consumer group name
     * @param topicName -> topic name
     * @param offsets -> OffsetsInitializer (earliest or latest)
     * @return
     */
    public static KafkaSource<String> kafkaSource(String kafkaGroup, String topicName, OffsetsInitializer offsets) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setGroupId(kafkaGroup)
                .setTopics(topicName)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(offsets)
                .build();
    }
}
