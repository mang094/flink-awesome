package com.hw.news.pulsar;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.pulsar.source.PulsarSource;

/**
 * @ClassName CommonPulsarSourceHelper
 * @Description
 * @Author zh
 * @Date 2023/12/28 16:17
 * @Version
 */
public class CommonPulsarSourceHelper {

    private CommonPulsarSourceHelper() {}


    public static PulsarSource<String> pulsarSource(String topicName, OffsetsInitializer offsets) {
       return  PulsarSource.builder()
               .setServiceUrl("serviceUrl")
               .setAdminUrl("adminUrl")
               .setTopics("topic1")
               .setDeserializationSchema(new SimpleStringSchema())
               .build();
           }


}

