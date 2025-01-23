//package com.hw.data.job;
//
//import com.ververica.cdc.connectors.mysql.source.MySqlSource;
//import com.ververica.cdc.connectors.mysql.table.StartupOptions;
//import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//
//
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//
//import java.util.Properties;
//
///**
// * @ClassName test1
// * @Description TODO
// * @Author zh
// * @Date 2024/7/26 14:10
// * @Version
// */
//public class test1 {
//
//    public static void main(String[] args) throws Exception {
//        // 1.获取flink执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//        // 2.开启checkpoint
//
//        //env.enableCheckpointing(5000);
//        //env.getCheckpointConfig().setCheckpointTimeout(10000);
//        //env.getCheckpointConfig().setCheckpointStorage("hdfs");//存储位置
//
//        //env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE); //精准一次
//
//        //env.getCheckpointConfig().setMaxConcurrentCheckpoints(1); //并发检查点数 同时只存在一个
//
//        // 3.构建pgsource读取cdc数据
//
////        Properties properties = new Properties();
////        properties.setProperty("snapshot.mode", "initial");
////        properties.setProperty("debezium.slot.name", "pg_cdc");
////        properties.setProperty("debezium.slot.drop.on.stop", "true");
////        properties.setProperty("include.schema.changes", "true");
//        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
//                .hostname("127.0.0.1")
//                .port(3306)
//                .databaseList("test")
//                .tableList("test.user_info")
//                .username("root")
//                .password("123456")
//                .startupOptions(StartupOptions.initial())
//                .deserializer(new JsonDebeziumDeserializationSchema())
//
//                .build();
//        DataStreamSource<String> mysqlDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(),"MySql-Source");
//        mysqlDS.print();
//        env.execute("FlinkCdsDataStream");
//    }
//}
//
