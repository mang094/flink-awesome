package com.hw.news.task.test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.hw.news.util.UserInfo;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ClassName test1
 * @Description cdc 2.0 + flink1.17
 * @Author zh
 * @Date
 * @Version
 */
public class MySqlCdcToEs {
    private static final String INDEX_NAME = "user_info_v1";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
            .withZone(ZoneId.of("UTC"));
    
    public static void main(String[] args) throws Exception {
        // 初始化ES索引
        initEsIndex();
        // 1.获取flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("127.0.0.1")
                .port(3306)
                .databaseList("test")
                .tableList("test.user_info")
                .username("root")
                .password("123456")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
        DataStreamSource<String> mysqlDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(),"MySql-Source");
        mysqlDS.print();

        SingleOutputStreamOperator<UserInfo> userInfoStream = mysqlDS.process(new ProcessFunction<String, UserInfo>() {
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

        // 配置ES连接
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("localhost", 9200, "http"));

        // 创建ES sink function
        ElasticsearchSinkFunction<UserInfo> elasticsearchSinkFunction = new ElasticsearchSinkFunction<UserInfo>() {
            @Override
            public void process(UserInfo element, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                try {
                    if (element.getName() == null && element.getAge() == null && element.getEmail() == null) {
                        // 处理删除操作
                        DeleteRequest deleteRequest = new DeleteRequest(INDEX_NAME)
                                .id(String.valueOf(element.getId()));
                        requestIndexer.add(deleteRequest);
                    } else {
                        // 处理新增和更新操作
                        Map<String, Object> document = new HashMap<>();
                        document.put("id", element.getId());
                        document.put("name", element.getName());
                        document.put("age", element.getAge());
                        document.put("email", element.getEmail());
                        // 格式化时间为ISO 8601格式
                        document.put("update_time", DATE_FORMATTER.format(element.getUpdateTime().toInstant()));

                        IndexRequest indexRequest = Requests.indexRequest()
                                .index(INDEX_NAME)
                                .id(String.valueOf(element.getId()))
                                .source(document, XContentType.JSON);
                        requestIndexer.add(indexRequest);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };

        // 创建ES sink builder
        ElasticsearchSink.Builder<UserInfo> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                elasticsearchSinkFunction
        );

        // 配置ES sink
        esSinkBuilder.setBulkFlushMaxActions(1000);  // 增加批量写入的大小
        esSinkBuilder.setBulkFlushInterval(5000L);   // 每5秒刷新一次
        esSinkBuilder.setBulkFlushBackoff(true);     // 启用重试机制
        esSinkBuilder.setBulkFlushBackoffType(ElasticsearchSinkBase.FlushBackoffType.EXPONENTIAL);  // 使用指数退避
        esSinkBuilder.setBulkFlushBackoffRetries(3); // 最大重试次数
        esSinkBuilder.setBulkFlushBackoffDelay(1000L); // 初始延迟时间
        
        esSinkBuilder.setFailureHandler(new ActionRequestFailureHandler() {
            @Override
            public void onFailure(ActionRequest action, Throwable failure, int restStatusCode, RequestIndexer indexer) throws Throwable {
                if (failure instanceof Exception) {
                    // 打印错误信息
                    //System.err.println("Error while indexing: " + failure.getMessage());
                    failure.printStackTrace();
                    // 重试该请求
                    //indexer.add(action);
                } else {
                    throw failure;
                }
            }
        });

        // 添加sink
        userInfoStream.addSink(esSinkBuilder.build()).name("Elasticsearch Sink");

        env.execute("FlinkCdsDataStream");
    }

    private static void initEsIndex() throws Exception {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http")));

        try {
            // Check if index exists
            boolean exists = client.indices().exists(new GetIndexRequest(INDEX_NAME), RequestOptions.DEFAULT);

            if (!exists) {
                CreateIndexRequest request = new CreateIndexRequest(INDEX_NAME);

                // Define mapping using XContentBuilder
                XContentBuilder builder = XContentFactory.jsonBuilder();
                builder.startObject();
                {
                    builder.startObject("properties");
                    {
                        builder.startObject("id").field("type", "long").endObject();
                        builder.startObject("name").field("type", "keyword").endObject();
                        builder.startObject("age").field("type", "integer").endObject();
                        builder.startObject("email").field("type", "keyword").endObject();
                        builder.startObject("update_time")
                                .field("type", "date")
                                .field("format", "strict_date_time||strict_date_optional_time||epoch_millis")
                                .endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();

                request.mapping(builder);

                // Set index settings
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
