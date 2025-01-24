package com.hw.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

public class CommonMySQLCdcSourceHelper {

    private CommonMySQLCdcSourceHelper() {
        // DO NOTHING
    }

    public static MySqlSource<String> buildMySqlSource(String hostname,
                                                       int port,
                                                       String database,
                                                       String table,
                                                       String username,
                                                       String password) {
        return MySqlSource.<String>builder()
                .hostname(hostname)
                .port(port)
                .databaseList(database)
                .tableList(table)
                .username(username)
                .password(password)
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())  // 将 SourceRecord 转换为 JSON 字符串
                .build();
    }

}
