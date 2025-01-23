package com.hw;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;

public class MySqlSourceExample {
  public static void main(String[] args) throws Exception {
    MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
        .hostname("127.0.0.1")
        .port(3306)
        .databaseList("test") // 设置捕获的数据库， 如果需要同步整个数据库，请将 tableList 设置为 ".*".
        .tableList("test.user_info") // 设置捕获的表
        .username("root")
        .password("123456")
            .startupOptions(StartupOptions.initial())
        .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
        .build();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    // 设置 3s 的 checkpoint 间隔
    env.enableCheckpointing(3000);

    env
      .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
      // 设置 source 节点的并行度为 4
      .setParallelism(4)
      .print().setParallelism(1); // 设置 sink 节点并行度为 1 

    env.execute("Print MySQL Snapshot + Binlog");
  }
}