package com.hw.news.task.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class PostgresToPostgresSync {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 配置PostgreSQL连接参数
        String sourceDDL = "CREATE TABLE ads_investment_signal ( " +
                "                company_code varchar(255) NOT NULL," +
                "                company_name varchar(255) NULL," +
                "                iconurl varchar(255) NULL, " +
                "                company_sname varchar(255) NULL, " +
                "                industry varchar(255) NULL, " +
                "                total_score DOUBLE NULL, " +
                "                label_level_two varchar(255) NULL, " +
                "                publish_time timestamp NULL, " +
                "                title varchar NULL, " +
                "                summary varchar(255) NULL, " +
                "                content varchar NULL, " +
                "                content_confirm varchar NULL, " +
                "                detail_url varchar NULL, " +
                "                site_name varchar(255) NULL, " +
                "                update_time timestamp NULL , " +

                "                is_delete int NOT NULL , " +
                "                label_level_one varchar(255) NULL, " +
                "                PRIMARY KEY (company_code) NOT ENFORCED  " +
                "                )  WITH (" +
                "  'connector' = 'postgres-cdc'," +
                "  'hostname' = '192.168.200.69'," +
                "  'port' = '5432'," +
                "  'username' = 'brain_saas'," +
                "  'password' = 'bjbsHt2oY)l2V40j'," +
                "  'database-name' = 'brain_saas'," +
                "  'schema-name' = 'ads'," +
                "  'table-name' = 'ads_investment_signal'" +
                ")";

        String sinkDDL = "CREATE TABLE ads_investment_signal_v10 ( " +
                "                company_code varchar(255) NOT NULL, " +
                "                company_name varchar(255) NULL, " +
                "                iconurl varchar(255) NULL, " +
                "                company_sname varchar(255) NULL, " +
                "                industry varchar(255) NULL, " +
                "                total_score DOUBLE NULL, " +
                "                label_level_two varchar(255) NULL, " +
                "                publish_time timestamp NULL," +
                "                title varchar NULL, " +
                "                summary varchar(255) NULL, " +
                "                content varchar NULL, " +
                "                content_confirm varchar NULL, " +
                "                detail_url varchar NULL, " +
                "                site_name varchar(255) NULL, " +
                "                update_time timestamp NULL , " +

                "                is_delete int NOT NULL , " +
                "                label_level_one varchar(255) NULL, " +
                "                PRIMARY KEY (company_code) NOT ENFORCED  " +
                "                )  WITH (" +
                "  'connector' = 'postgres'," +
                "  'hostname' = '192.168.200.69'," +
                "  'port' = '5432'," +
                "  'username' = 'brain_saas'," +
                "  'password' = 'bjbsHt2oY)l2V40j'," +
                "  'database-name' = 'brain_saas'," +
                "  'schema-name' = 'ads'," +
                "  'table-name' = 'ads_investment_signal_v10'" +
                ")";

        tableEnv.executeSql(sourceDDL);
        tableEnv.executeSql(sinkDDL);

        Table result = tableEnv.from("ads_investment_signal");
        tableEnv.createTemporaryView("ads_investment_signal_v10", result);

        // 将sourceTable的数据实时同步到sinkTable
        tableEnv.executeSql("INSERT INTO ads_investment_signal SELECT * FROM ads_investment_signal_v10");

        env.execute("Postgres to Postgres Sync Job");
    }
}
