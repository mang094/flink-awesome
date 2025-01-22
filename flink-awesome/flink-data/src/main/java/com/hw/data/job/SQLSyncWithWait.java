package com.hw.data.job;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 使用SQL方式实现MySQL CDC同步
 * 特点:
 * 1. 使用INNER JOIN等待两表数据都到达后再同步
 * 2. 使用CDC实时捕获MySQL变更
 * 3. 支持增量和全量同步
 */
public class SQLSyncWithWait {
    private static final Logger LOG = LoggerFactory.getLogger(SQLSyncWithWait.class);

    public static void main(String[] args) {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 启用检查点,确保数据一致性
        env.enableCheckpointing(5000);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 设置CDC参数
        tableEnv.executeSql("SET 'execution.checkpointing.interval' = '5s'");
        tableEnv.executeSql("SET 'table.exec.source.idle-timeout' = '10s'");

        try {
            // 创建主表源表
            LOG.info("创建主表 CDC 源表");
            tableEnv.executeSql(
                "CREATE TABLE main_table (" +
                "    id BIGINT," +
                "    main_data STRING," +
                "    ts TIMESTAMP_LTZ(3)," + // 使用TIMESTAMP类型
                "    PRIMARY KEY (id) NOT ENFORCED" +
                ") WITH (" +
                "    'connector' = 'mysql-cdc'," +
                "    'hostname' = 'localhost'," +
                "    'port' = '3306'," +
                "    'username' = 'root'," +
                "    'password' = '123456'," +
                "    'database-name' = 'test'," +
                "    'table-name' = 'main_table'," +
                "    'scan.startup.mode' = 'initial'" + // 设置启动模式为initial
                ")"
            );

            // 创建副表源表
            LOG.info("创建副表 CDC 源表");
            tableEnv.executeSql(
                "CREATE TABLE sub_table (" +
                "    id BIGINT," +
                "    main_table_id BIGINT," +
                "    sub_data STRING," +
                "    ts TIMESTAMP_LTZ(3)," +
                "    PRIMARY KEY (id) NOT ENFORCED" +
                ") WITH (" +
                "    'connector' = 'mysql-cdc'," +
                "    'hostname' = 'localhost'," +
                "    'port' = '3306'," +
                "    'username' = 'root'," +
                "    'password' = '123456'," +
                "    'database-name' = 'test'," +
                "    'table-name' = 'sub_table'," +
                "    'scan.startup.mode' = 'initial'" +
                ")"
            );

            // 创建目标表
            LOG.info("创建目标表");
            tableEnv.executeSql(
                "CREATE TABLE target_table (" +
                "    id BIGINT," +
                "    main_data STRING," +
                "    sub_data STRING," +
                "    ts TIMESTAMP_LTZ(3)," +
                "    PRIMARY KEY (id) NOT ENFORCED" +
                ") WITH (" +
                "    'connector' = 'jdbc'," +
                "    'url' = 'jdbc:mysql://localhost:3306/test?useSSL=false'," +
                "    'table-name' = 'target_table'," +
                "    'username' = 'root'," +
                "    'password' = '123456'," +
                "    'sink.buffer-flush.max-rows' = '1'," + // 设置缓冲区大小
                "    'sink.buffer-flush.interval' = '0s'," + // 实时刷新
                "    'sink.max-retries' = '3'" + // 最大重试次数
                ")"
            );

            // 执行同步查询
            LOG.info("开始执行数据同步");
            String insertSQL = 
                "INSERT INTO target_table " +
                "SELECT " +
                "    m.id, " +
                "    m.main_data, " +
                "    s.sub_data, " +
                "    COALESCE(m.ts, s.ts) as ts " + // 使用最新时间戳
                "FROM main_table m " +
                "LEFT JOIN sub_table s " + // 改用LEFT JOIN允许副表数据延迟到达
                "ON m.id = s.main_table_id";
            
            tableEnv.executeSql(insertSQL);

        } catch (Exception e) {
            LOG.error("执行同步时发生错误: ", e);
            throw e;
        }
    }
}