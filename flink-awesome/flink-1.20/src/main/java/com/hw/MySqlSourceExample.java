package com.hw;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hw.source.CommonMySQLCdcSourceHelper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class MySqlSourceExample {
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
                        UserInfo userInfo = new UserInfo();
                        userInfo.setId(after.get("id").asLong());
                        userInfo.setName(after.get("name").asText());
                        userInfo.setAge(after.get("age").asInt());
                        userInfo.setEmail(after.get("email").asText());
                        out.collect(userInfo);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).name("Process CDC Data");
        // 添加MySQL sink
        String targetSql = "INSERT INTO user_info_1 (id, name, age, email) VALUES (?, ?, ?, ?) " +
                          "ON DUPLICATE KEY UPDATE name=VALUES(name), age=VALUES(age), email=VALUES(email)";
        userInfoStream.addSink(JdbcSink.<UserInfo>sink(
            targetSql,
            (statement, userInfo) -> {
                statement.setLong(1, userInfo.getId());
                statement.setString(2, userInfo.getName());
                statement.setInt(3, userInfo.getAge());
                statement.setString(4, userInfo.getEmail());
            },
            new JdbcExecutionOptions.Builder()
                .withBatchSize(1000)
                .withBatchIntervalMs(200)
                .withMaxRetries(3)
                .build(),
            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:mysql://localhost:3306/test")
                .withDriverName("com.mysql.cj.jdbc.Driver")
                .withUsername("root")
                .withPassword("123456")
                .build()
        )).name("MySQL Sink");
    env.execute("Print MySQL Snapshot + Binlog");
  }
}