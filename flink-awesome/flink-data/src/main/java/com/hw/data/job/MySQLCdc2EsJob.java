package com.hw.data.job;

import com.hw.connectors.elasticsearch.ElasticsearchHttpSink;
import com.hw.data.model.UserInfo;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RuntimeContext;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.sql.Timestamp;
import java.util.*;

import static org.elasticsearch.client.Requests.createIndexRequest;

public class MySQLCdc2EsJob {
    private static final String INDEX_NAME = "user_info";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        // 初始化ES索引
        initEsIndex();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        // 配置MySQL CDC Source
        Properties debeziumProperties = new Properties();
        debeziumProperties.put("decimal.handling.mode", "string");
        debeziumProperties.put("include.schema.changes", "false");
        debeziumProperties.put("snapshot.mode", "initial");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("127.0.0.1")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("test")
                .tableList("test.user_info")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .debeziumProperties(debeziumProperties)
                .build();

        // 创建数据处理流程
        DataStreamSource<String> source = env.fromSource(mySqlSource,
                WatermarkStrategy.noWatermarks(),
                "MySQL CDC Source");
                
        // 打印源数据用于调试
        source.print().name("Source Data");

        // 转换CDC JSON数据为UserInfo对象
//        SingleOutputStreamOperator<UserInfo> userInfoStream = source.process(new ProcessFunction<String, UserInfo>() {
//            @Override
//            public void processElement(String value, Context ctx, Collector<UserInfo> out) throws Exception {
//                try {
//                    JsonNode jsonNode = OBJECT_MAPPER.readTree(value);
//                    String op = jsonNode.get("op").asText();
//                    System.out.println("Processing CDC event: " + value);
//
//                    switch (op) {
//                        case "r": // 插入
//                        case "c": // 插入
//                        case "u": // 更新
//                            if (jsonNode.has("after") && !jsonNode.get("after").isNull()) {
//                                JsonNode after = jsonNode.get("after");
//                                UserInfo userInfo = new UserInfo();
//                                userInfo.setId(after.get("id").asLong());
//                                userInfo.setName(after.get("name").asText());
//                                userInfo.setAge(after.get("age").asInt());
//                                userInfo.setEmail(after.get("email").asText());
//                                String updateTimeStr = after.get("update_time").asText();
//                                updateTimeStr = updateTimeStr.replace('T', ' ')
//                                                          .replace('Z', ' ')
//                                                          .trim();
//                                System.out.println("Emitting UserInfo: " + userInfo);
//                                out.collect(userInfo);
//                            }
//                            break;
//                        case "d": // 删除
//                            if (jsonNode.has("before") && !jsonNode.get("before").isNull()) {
//                                JsonNode before = jsonNode.get("before");
//                                UserInfo userInfo = new UserInfo();
//                                userInfo.setId(before.get("id").asLong());
//                                System.out.println("Emitting delete event for ID: " + userInfo.getId());
//                                out.collect(userInfo);
//                            }
//                            break;
//                    }
//                } catch (Exception e) {
//                    System.err.println("Error processing CDC event: " + e.getMessage());
//                    e.printStackTrace();
//                }
//            }
//        }).name("Process CDC Data");
        SingleOutputStreamOperator<UserInfo> userInfoStream = source.process(new ProcessFunction<String, UserInfo>() {
            @Override
            public void processElement(String value, Context ctx, Collector<UserInfo> out) {
                try {
                    JsonNode jsonNode = OBJECT_MAPPER.readTree(value);
                    JsonNode after = jsonNode.get("after");
                    JsonNode before = jsonNode.get("before");
                    String op = jsonNode.get("op").asText();

                    if ("d".equals(op)) {
                        // 处理删除操作
                        UserInfo userInfo = new UserInfo();
                        userInfo.setId(before.get("id").asLong());
                        out.collect(userInfo);
                    } else if (after != null) {
                        // 处理插入和更新操作
                        System.out.println("..................................");
                        UserInfo userInfo = new UserInfo();
                        userInfo.setId(after.get("id").asLong());
                        userInfo.setName(after.get("name").asText());
                        userInfo.setAge(after.get("age").asInt());
                        userInfo.setEmail(after.get("email").asText());
                                                        String updateTimeStr = after.get("update_time").asText();
                                updateTimeStr = updateTimeStr.replace('T', ' ')
                                                          .replace('Z', ' ')
                                                          .trim();
                        userInfo.setUpdateTime(Timestamp.valueOf(updateTimeStr));
                        out.collect(userInfo);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).name("Process CDC Data");

        // 打印转换后的数据用于调试
        userInfoStream.print().name("Processed Data");

        // 创建ES sink
//        ElasticsearchSink.Builder<UserInfo> esSinkBuilder = new ElasticsearchSink.Builder<>(
//            Collections.singletonList(new HttpHost("localhost", 9200, "http")),
//            new ElasticsearchSinkFunction<UserInfo>() {
//                @Override
//                public void process(UserInfo element, RuntimeContext ctx, RequestIndexer indexer) {
//                    try {
//                        System.out.println("Debug - Element received in ES sink: " + element);
//                        System.out.println("Debug - Element properties: id=" + element.getId() +
//                                         ", name=" + element.getName() +
//                                         ", age=" + element.getAge() +
//                                         ", email=" + element.getEmail());
//
//                        if (element.getName() == null && element.getAge() == null && element.getEmail() == null) {
//                            System.out.println("Debug - Processing delete operation for ID: " + element.getId());
//                            DeleteRequest request = new DeleteRequest(INDEX_NAME)
//                                    .id(String.valueOf(element.getId()));
//                            indexer.add(request);
//                            System.out.println("Debug - Delete request added to indexer");
//                        } else {
//                            System.out.println("Debug - Processing insert/update operation");
//                            Map<String, Object> document = new HashMap<>();
//                            document.put("id", element.getId());
//                            document.put("name", element.getName());
//                            document.put("age", element.getAge());
//                            document.put("email", element.getEmail());
//
//                            IndexRequest request = Requests.indexRequest()
//                                    .index(INDEX_NAME)
//                                    .id(String.valueOf(element.getId()))
//                                    .source(document);
//                            indexer.add(request);
//                            System.out.println("Debug - Index request added to indexer");
//                        }
//                    } catch (Exception e) {
//                        System.err.println("Error in ES sink: " + e.getMessage());
//                        e.printStackTrace();
//                    }
//                }
//            }
//        );
//
//        // 配置ES sink
//        esSinkBuilder.setBulkFlushMaxActions(1);
//        esSinkBuilder.setBulkFlushInterval(1000L);
//        esSinkBuilder.setBulkFlushBackoff(true);
//        esSinkBuilder.setBulkFlushBackoffType(ElasticsearchSink.FlushBackoffType.EXPONENTIAL);
//        esSinkBuilder.setBulkFlushBackoffRetries(3);
//        esSinkBuilder.setBulkFlushBackoffDelay(1000L);


        userInfoStream.addSink(new ElasticsearchHttpSink());

        // 添加sink并确保取消注释
        // 这里有两次build()调用,导致第二次调用会失败
        // ElasticsearchSink<UserInfo> build = esSinkBuilder.build(); // 删除这行
        //userInfoStream.addSink(esSinkBuilder.build()).name("Elasticsearch Sink");

        // 设置检查点
        env.enableCheckpointing(3000);

        env.execute("MySQL CDC to Elasticsearch Sync Job");
    }

    private static IndexRequest createIndexRequest(UserInfo element) {
        Map<String, Object> document = new HashMap<>();
        document.put("id", element.getId());
        document.put("name", element.getName());
        document.put("age", element.getAge());
        document.put("email", element.getEmail());

        return Requests.indexRequest()
                .index(INDEX_NAME)
                .id(String.valueOf(element.getId()))
                .source(document);
    }

    private static void initEsIndex() throws Exception {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http")));

        try {
            // 检查索引是否存在
            boolean exists = client.indices().exists(new GetIndexRequest(INDEX_NAME), RequestOptions.DEFAULT);

            if (!exists) {
                // 创建索引
                CreateIndexRequest request = new CreateIndexRequest(INDEX_NAME);

                // 设置索引mapping
                XContentBuilder mapping = XContentFactory.jsonBuilder()
                    .startObject()
                        .startObject("properties")
                            .startObject("id")
                                .field("type", "long")
                            .endObject()
                            .startObject("name")
                                .field("type", "keyword")
                            .endObject()
                            .startObject("age")
                                .field("type", "integer")
                            .endObject()
                            .startObject("email")
                                .field("type", "keyword")
                            .endObject()
                            .startObject("update_time")
                                .field("type", "date")
                                .field("format", "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis")
                            .endObject()
                        .endObject()
                    .endObject();

                request.mapping(mapping);

                // 设置索引配置
                request.settings(Settings.builder()
                        .put("index.number_of_shards", 3)
                        .put("index.number_of_replicas", 1)
                );

                client.indices().create(request, RequestOptions.DEFAULT);
                System.out.println("ES index created successfully");
            }
        } catch (Exception e) {
            System.err.println("Error initializing ES index: " + e.getMessage());
            e.printStackTrace();
            throw e;
        } finally {
            client.close();
        }
    }
}