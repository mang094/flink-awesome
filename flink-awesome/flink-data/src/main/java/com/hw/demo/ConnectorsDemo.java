//package com.hw.demo;
//
//import com.hw.connectors.elasticsearch.ElasticsearchConnectorUtil;
//import com.hw.connectors.kafka.KafkaConnectorUtil;
//import com.hw.connectors.jdbc.MySQLConnectorUtil;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.http.HttpHost;
//import org.elasticsearch.action.index.IndexRequest;
//import org.elasticsearch.common.xcontent.XContentType;
//
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.Map;
//
//public class ConnectorsDemo {
//    public static void main(String[] args) throws Exception {
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        // Kafka Source
//        String bootstrapServers = "localhost:9092";
//        String sourceTopic = "source-topic";
//        String sinkTopic = "sink-topic";
//        String groupId = "flink-group";
//
//        // Create Kafka source
//        var kafkaSource = KafkaConnectorUtil.createKafkaSource(bootstrapServers, sourceTopic, groupId);
//
//        // Create Kafka sink
//        var kafkaSink = KafkaConnectorUtil.createKafkaSink(bootstrapServers, sinkTopic);
//
//        // MySQL configuration
//        String jdbcUrl = "jdbc:mysql://localhost:3306/test";
//        String username = "root";
//        String password = "password";
//
//        // Create MySQL sink
//        var jdbcConnectionOptions = MySQLConnectorUtil.createConnectionOptionsBuilder(
//                jdbcUrl, username, password).build();
//
//        var mysqlSink = MySQLConnectorUtil.createJdbcSink(
//                "INSERT INTO users (name, age) VALUES (?, ?)",
//                (statement, value) -> {
//                    // Assuming value is a comma-separated string "name,age"
//                    String[] parts = ((String)value).split(",");
//                    statement.setString(1, parts[0]);
//                    statement.setInt(2, Integer.parseInt(parts[1]));
//                },
//                jdbcConnectionOptions
//        );
//
//        // Elasticsearch configuration
//        var httpHosts = Arrays.asList(
//                new HttpHost("localhost", 9200, "http")
//        );
//
//        // Create Elasticsearch sink
//        var elasticsearchSink = ElasticsearchConnectorUtil.createElasticsearchSink(
//                httpHosts,
//                "users",
//                value -> {
//                    // Assuming value is a comma-separated string "name,age"
//                    String[] parts = ((String)value).split(",");
//                    Map<String, Object> json = new HashMap<>();
//                    json.put("name", parts[0]);
//                    json.put("age", Integer.parseInt(parts[1]));
//
//                    return new IndexRequest("users")
//                            .source(json, XContentType.JSON);
//                }
//        );
//
//        // Create processing pipeline
//        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
//                .sinkTo(kafkaSink);
//
////        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
////                .addSink(mysqlSink);
////
////        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
////                .sinkTo(elasticsearchSink);
//
//        env.execute("Flink Connectors Demo");
//    }
//}