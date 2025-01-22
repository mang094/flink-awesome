//package com.hw.data.job;
//
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.api.java.typeutils.RowTypeInfo;
//import org.apache.flink.connector.jdbc.JdbcInputFormat;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.http.HttpHost;
//import org.apache.flink.streaming.api.functions.sink.SinkFunction;
//import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
//import org.apache.flink.connector.elasticsearch.sink.ElasticsearchSink;
//import org.elasticsearch.action.index.IndexRequest;
//import org.elasticsearch.client.Requests;
//import org.apache.flink.types.Row;
//
//import java.util.Collections;
//import java.util.HashMap;
//import java.util.Map;
//
//public class FlinkMySQLToES {
//
//    public static void main(String[] args) throws Exception {
//        // 初始化 Flink 执行环境
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        // 配置 MySQL 读取
//        String[] fieldNames = {"id", "name", "age"};
//        TypeInformation<?>[] types = {TypeInformation.of(Integer.class), TypeInformation.of(String.class), TypeInformation.of(Integer.class)};
//        RowTypeInfo rowTypeInfo = new RowTypeInfo(types, fieldNames);
//
//        DataStream<Row> mysqlDataStream = env.createInput(
//                JdbcInputFormat.buildJdbcInputFormat()
//                        .setDrivername("com.mysql.cj.jdbc.Driver")
//                        .setDBUrl("jdbc:mysql://localhost:3306/your_database") // 替换为你的 MySQL 数据库地址
//                        .setUsername("your_username")                         // 替换为你的用户名
//                        .setPassword("your_password")                         // 替换为你的密码
//                        .setQuery("SELECT id, name, age FROM user")     // 替换为你的查询语句
//                        .setRowTypeInfo(rowTypeInfo)                          // 使用 RowTypeInfo
//                        .finish()
//        );
//
//        // 配置 Elasticsearch Sink
//        ElasticsearchSink<Row> esSink = new Elasticsearch7SinkBuilder<Row>()
//                .setHosts(new HttpHost("localhost", 9200, "http"))
//                .setEmitter((element, context, indexer) -> {
//                    Map<String, Object> json = new HashMap<>();
//                    json.put("id", element.getField(0));
//                    json.put("name", element.getField(1));
//                    json.put("age", element.getField(2));
//
//                    IndexRequest request = Requests.indexRequest()
//                            .index("user_index")
//                            .source(json);
//
//                    indexer.add(request);
//                })
//                .build();
//
//        // 数据流管道：从 MySQL 读取数据，写入到 Elasticsearch
//        mysqlDataStream.sinkTo(esSink);
//
//        // 启动任务
//        env.execute("Flink MySQL to Elasticsearch");
//    }
//
//    // 定义 MySQL 数据表对应的 POJO 类
//    public static class MySQLRow {
//        private int id;
//        private String name;
//        private int age;
//
//        public MySQLRow() {}
//
//        public MySQLRow(int id, String name, int age) {
//            this.id = id;
//            this.name = name;
//            this.age = age;
//        }
//
//        public int getId() {
//            return id;
//        }
//
//        public void setId(int id) {
//            this.id = id;
//        }
//
//        public String getName() {
//            return name;
//        }
//
//        public void setName(String name) {
//            this.name = name;
//        }
//
//        public int getAge() {
//            return age;
//        }
//
//        public void setAge(int age) {
//            this.age = age;
//        }
//    }
//}
