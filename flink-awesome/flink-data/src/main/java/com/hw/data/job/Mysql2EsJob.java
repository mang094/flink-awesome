//package com.hw.data.job;
//
//import com.hw.data.model.UserInfo;
//
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
//import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
//import org.apache.flink.connector.jdbc.JdbcSink;
//import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
//import org.apache.flink.streaming.api.functions.source.SourceFunction;
//import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
//import org.apache.flink.connector.elasticsearch.sink.ElasticsearchSink;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.http.HttpHost;
//import org.elasticsearch.action.index.IndexRequest;
//import org.elasticsearch.client.Requests;
//import org.elasticsearch.client.indices.CreateIndexRequest;
//import org.elasticsearch.client.indices.GetIndexRequest;
//import org.elasticsearch.client.RestHighLevelClient;
//import org.elasticsearch.client.RestClient;
//import org.elasticsearch.client.RequestOptions;
//import org.elasticsearch.common.settings.Settings;
//import org.elasticsearch.xcontent.XContentBuilder;
//import org.elasticsearch.xcontent.XContentFactory;
//
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.PreparedStatement;
//import java.sql.ResultSet;
//import java.sql.Timestamp;
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.Map;
//
//public class Mysql2EsJob {
//    private static final String INDEX_NAME = "user_info";
//    private static final String MYSQL_URL = "jdbc:mysql://localhost:3306/test";
//    private static final String MYSQL_USERNAME = "root";
//    private static final String MYSQL_PASSWORD = "password";
//
//    public static void main(String[] args) throws Exception {
//        // 初始化ES索引
//        initEsIndex();
//
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        // 创建MySQL source
//        JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//                .withUrl(MYSQL_URL)
//                .withUsername(MYSQL_USERNAME)
//                .withPassword(MYSQL_PASSWORD)
//                .withDriverName("com.mysql.cj.jdbc.Driver")
//                .build();
//
//        JdbcExecutionOptions executionOptions = JdbcExecutionOptions.builder()
//                .withBatchSize(1000)
//                .build();
//
//        // 创建ES sink
//        ElasticsearchSink<UserInfo> esSink = new Elasticsearch7SinkBuilder<UserInfo>()
//                .setHosts(new HttpHost("localhost", 9200, "http"))
//                .setEmitter((element, context, indexer) -> {
//                    Map<String, Object> document = new HashMap<>();
//                    document.put("id", element.getId());
//                    document.put("name", element.getName());
//                    document.put("age", element.getAge());
//                    document.put("email", element.getEmail());
//                    document.put("update_time", element.getUpdateTime());
//
//                    IndexRequest request = Requests.indexRequest()
//                            .index(INDEX_NAME)
//                            .id(String.valueOf(element.getId()))
//                            .source(document);
//
//                    indexer.add(request);
//                })
//                .setBulkFlushMaxActions(1000)
//                .setBulkFlushInterval(5000)
//                .setBulkFlushMaxSizeMb(5)
//                .build();
//
//        // 创建数据处理流程
//        env.addSource(new MySQLSourceFunction(connectionOptions))
//                .uid("mysql-source")
//                .name("MySQL Source")
//                .sinkTo(esSink)
//                .uid("es-sink")
//                .name("Elasticsearch Sink");
//
//        env.execute("MySQL to Elasticsearch Sync Job");
//    }
//
//    private static class MySQLSourceFunction implements SourceFunction<UserInfo> {
//        private volatile boolean isRunning = true;
//        private final JdbcConnectionOptions connectionOptions;
//        private Connection connection;
//        private PreparedStatement statement;
//        private Timestamp lastUpdateTime;
//
//        public MySQLSourceFunction(JdbcConnectionOptions connectionOptions) {
//            this.connectionOptions = connectionOptions;
//            this.lastUpdateTime = new Timestamp(0);  // 从1970年开始
//        }
//
//        @Override
//        public void run(SourceContext<UserInfo> ctx) throws Exception {
//            connection = DriverManager.getConnection(
//                    connectionOptions.getDbURL(),
//                    String.valueOf(connectionOptions.getUsername()),
//                    String.valueOf(connectionOptions.getPassword())
//            );
//
//            while (isRunning) {
//                statement = connection.prepareStatement(
//                        "SELECT id, name, age, email, update_time FROM user_info " +
//                        "WHERE update_time > ? ORDER BY update_time ASC LIMIT 1000"
//                );
//                statement.setTimestamp(1, lastUpdateTime);
//
//                ResultSet resultSet = statement.executeQuery();
//                boolean hasData = false;
//
//                while (resultSet.next()) {
//                    hasData = true;
//                    UserInfo userInfo = new UserInfo();
//                    userInfo.setId(resultSet.getLong("id"));
//                    userInfo.setName(resultSet.getString("name"));
//                    userInfo.setAge(resultSet.getInt("age"));
//                    userInfo.setEmail(resultSet.getString("email"));
//                    userInfo.setUpdateTime(resultSet.getTimestamp("update_time"));
//
//                    lastUpdateTime = userInfo.getUpdateTime();
//                    ctx.collect(userInfo);
//                }
//
//                resultSet.close();
//                statement.close();
//
//                if (!hasData) {
//                    Thread.sleep(5000); // 如果没有新数据，等待5秒
//                }
//            }
//        }
//
//        @Override
//        public void cancel() {
//            isRunning = false;
//            try {
//                if (statement != null) {
//                    statement.close();
//                }
//                if (connection != null) {
//                    connection.close();
//                }
//            } catch (Exception e) {
//                // 忽略关闭异常
//            }
//        }
//    }
//
//    private static String buildQuery() {
//        return "SELECT id, name, age, email, update_time FROM user_info " +
//               "WHERE update_time > ? " +
//               "ORDER BY update_time ASC";
//    }
//
//    private static void initEsIndex() throws Exception {
//        RestHighLevelClient client = new RestHighLevelClient(
//                RestClient.builder(new HttpHost("localhost", 9200, "http")));
//
//        try {
//            // 检查索引是否存在
//            boolean exists = client.indices().exists(new GetIndexRequest(INDEX_NAME), RequestOptions.DEFAULT);
//
//            if (!exists) {
//                // 创建索引
//                CreateIndexRequest request = new CreateIndexRequest(INDEX_NAME);
//
//                // 设置索引mapping
//                XContentBuilder mapping = XContentFactory.jsonBuilder()
//                    .startObject()
//                        .startObject("properties")
//                            .startObject("id")
//                                .field("type", "long")
//                            .endObject()
//                            .startObject("name")
//                                .field("type", "keyword")
//                            .endObject()
//                            .startObject("age")
//                                .field("type", "integer")
//                            .endObject()
//                            .startObject("email")
//                                .field("type", "keyword")
//                            .endObject()
//                            .startObject("update_time")
//                                .field("type", "date")
//                                .field("format", "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis")
//                            .endObject()
//                        .endObject()
//                    .endObject();
//
//                request.mapping(mapping);
//
//                // 设置索引配置
//                request.settings(Settings.builder()
//                        .put("index.number_of_shards", 3)
//                        .put("index.number_of_replicas", 1)
//                );
//
//                client.indices().create(request, RequestOptions.DEFAULT);
//            }
//        } finally {
//            client.close();
//        }
//    }
//}