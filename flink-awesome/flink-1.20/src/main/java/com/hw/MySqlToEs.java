package com.hw;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hw.source.CommonMySQLCdcSourceHelper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.connector.elasticsearch.sink.FlushBackoffType;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import com.alibaba.fastjson.JSON;
import java.util.HashMap;
import java.util.Map;

public class MySqlToEs {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public static void main(String[] args) throws Exception {

        MySqlSource<String> mySqlSource = CommonMySQLCdcSourceHelper
                .buildMySqlSource("127.0.0.1",3306,"test","test.user_info","root","123456");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 设置 3s 的 checkpoint 间隔
        env.enableCheckpointing(3000);

        DataStreamSource<String> mySQLSource = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // 设置 source 节点的并行度为 4
                .setParallelism(4);
        mySQLSource.print().setParallelism(1); // 设置 sink 节点并行度为 1
        // 转换为UserInfo对象
        SingleOutputStreamOperator<UserInfo> userInfoStream = mySQLSource.process(new ProcessFunction<String, UserInfo>() {
            @Override
            public void processElement(String value, Context ctx, Collector<UserInfo> out) {
                try {
                    JsonNode jsonNode = OBJECT_MAPPER.readTree(value);
                    if (jsonNode == null) {
                        return;
                    }
                    
                    JsonNode after = jsonNode.get("after");
                    JsonNode before = jsonNode.get("before");
                    JsonNode opNode = jsonNode.get("op");
                    
                    if (opNode == null) {
                        return;
                    }

                    
                    String op = opNode.asText();

                    if ("d".equals(op) && before != null) {
                        // 处理删除操作
                        UserInfo userInfo = new UserInfo();
                        JsonNode idNode = before.get("id");
                        if (idNode != null) {
                            userInfo.setId(idNode.asLong());
                            out.collect(userInfo);
                        }
                    } else if (after != null) {
                        // 处理插入和更新操作
                        UserInfo userInfo = new UserInfo();
                        JsonNode idNode = after.get("id");
                        JsonNode nameNode = after.get("name");
                        JsonNode ageNode = after.get("age");
                        JsonNode emailNode = after.get("email");
                        
                        if (idNode != null) {
                            userInfo.setId(idNode.asLong());
                        }
                        if (nameNode != null && !nameNode.isNull()) {
                            userInfo.setName(nameNode.asText());
                        }
                        if (ageNode != null && !ageNode.isNull()) {
                            userInfo.setAge(ageNode.asInt());
                        }
                        if (emailNode != null && !emailNode.isNull()) {
                            userInfo.setEmail(emailNode.asText());
                        }
                        out.collect(userInfo);
                    }
                } catch (Exception e) {
                    // Log the error but don't throw it to prevent pipeline failure
                    System.err.println("Error processing record: " + value);
                    e.printStackTrace();
                }
            }
        }).name("Process CDC Data");

        // 添加Elasticsearch sink
        userInfoStream.sinkTo(
                new Elasticsearch7SinkBuilder<UserInfo>()
                        .setHosts(new HttpHost("127.0.0.1", 9200, "http"))
                        .setEmitter((element, context, indexer) -> {
                            try {
                                Map<String, Object> document = new HashMap<>();
                                document.put("id", element.getId());
                                document.put("name", element.getName());
                                document.put("age", element.getAge());
                                document.put("email", element.getEmail());
                                
                                IndexRequest request = Requests.indexRequest()
                                        .index("user_info")  // 指定索引名称
                                        .id(String.valueOf(element.getId()))  // 使用用户ID作为文档ID
                                        .source(document);  // 使用Map作为source
                                indexer.add(request);
                            } catch (Exception e) {
                                System.err.println("Error sending to Elasticsearch: " + element);
                                e.printStackTrace();
                            }
                        })
                        .setBulkFlushBackoffStrategy(FlushBackoffType.EXPONENTIAL, 5, 1000)
                        .build());

        env.execute("MySQL to Elasticsearch");
    }
}
