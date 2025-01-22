package com.hw.news.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

import static org.apache.flink.connector.base.DeliveryGuarantee.EXACTLY_ONCE;
import static org.apache.kafka.clients.producer.ProducerConfig.TRANSACTION_TIMEOUT_CONFIG;

public final class SaaSKafkaSInkHelper {
    public static final String BOOTSTRAP_SERVERS = "192.168.200.101:9092,192.168.200.102:9092,192.168.200.103:9092";
    public static final String TOPIC_NEWS_FIRST = "news-staging";
    public static final String TRANSACTION_PREFIX = "firestone-saas-";
    public static final String TRANSACTION_TIMEOUT = 10 * 60 * 1000 + ""; // 10 minutes
    private SaaSKafkaSInkHelper() {}

    public static KafkaSink<String> kafkaSink() {
        return KafkaSink.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                // 指定序列化器：指定topic名称，具体的序列化
                .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder()
                        .setTopic(TOPIC_NEWS_FIRST)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                // 写到kafka的一致性级别：精准一次，至少一次
                // 如果是精准一次，必须设置 事务前缀
                // 如果是精准一次，必须设置 事务超时时间：大于checkpoint间隔，小于max - 15分钟
                .setDeliveryGuarantee(EXACTLY_ONCE)
                .setTransactionalIdPrefix(TRANSACTION_PREFIX)
                .setProperty(TRANSACTION_TIMEOUT_CONFIG, TRANSACTION_TIMEOUT)
                .build();
    }

}
