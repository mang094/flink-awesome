//package com.hw.connectors.jdbc;
//
//import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
//import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
//import org.apache.flink.connector.jdbc.JdbcSink;
//import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
//import org.apache.flink.streaming.api.functions.sink.SinkFunction;
//
//public class MySQLConnectorUtil {
//
//    public static JdbcConnectionOptions.JdbcConnectionOptionsBuilder createConnectionOptionsBuilder(
//            String url, String username, String password) {
//        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//                .withUrl(url)
//                .withDriverName("com.mysql.cj.jdbc.Driver")
//                .withUsername(username)
//                .withPassword(password);
//    }
//
//    public static <T> SinkFunction<T> createJdbcSink(
//            String sql,
//            JdbcStatementBuilder<T> statementBuilder,
//            JdbcConnectionOptions connectionOptions) {
//
//        return JdbcSink.sink(
//                sql,
//                statementBuilder,
//                JdbcExecutionOptions.builder()
//                        .withBatchSize(1000)
//                        .withBatchIntervalMs(200)
//                        .withMaxRetries(3)
//                        .build(),
//                connectionOptions
//        );
//    }
//}