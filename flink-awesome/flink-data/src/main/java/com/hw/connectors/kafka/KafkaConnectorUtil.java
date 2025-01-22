//package com.hw.connectors.kafka;
//
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
//import org.apache.flink.connector.kafka.sink.KafkaSink;
//import org.apache.flink.connector.kafka.source.KafkaSource;
//import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
//
//import java.util.Properties;
//
//public class KafkaConnectorUtil {
//
//    public static KafkaSource<String> createKafkaSource(String bootstrapServers,
//                                                       String topic,
//                                                       String groupId) {
//        return KafkaSource.<String>builder()
//                .setBootstrapServers(bootstrapServers)
//                .setTopics(topic)
//                .setGroupId(groupId)
//                .setStartingOffsets(OffsetsInitializer.earliest())
//                .setValueOnlyDeserializer(new SimpleStringSchema())
//                .build();
//    }
//
//    public static KafkaSink<String> createKafkaSink(String bootstrapServers,
//                                                   String topic) {
//        return KafkaSink.<String>builder()
//                .setBootstrapServers(bootstrapServers)
//                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
//                        .setTopic(topic)
//                        .setValueSerializationSchema(new SimpleStringSchema())
//                        .build())
//                .build();
//    }
//}