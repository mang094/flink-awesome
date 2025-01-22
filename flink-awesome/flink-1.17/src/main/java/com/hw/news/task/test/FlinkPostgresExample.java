package com.hw.news.task.test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static com.hsmap.news.jdbc.SaasPostgresHelper.*;
import static com.hsmap.news.jdbc.SaasPostgresHelper.JDBC_USER_PASSWORD;


public class FlinkPostgresExample {
    public static void main(String[] args) throws Exception {
        // 设置执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(JDBC_URL_ADS)
                .withDriverName(JDBC_DRIVER_NAME)
                .withUsername(JDBC_USER_NAME)
                .withPassword(JDBC_USER_PASSWORD)
                .withConnectionCheckTimeoutSeconds(60)
                .build();
        // 定义 PostgreSQL 表的 DDL
        String createTableSql = "CREATE TABLE ads_investment_signal (" +
                "company_code varchar(255) NOT NULL," +
                "company_name varchar(255) NULL," +
                "iconurl varchar(255) NULL," +
                "company_sname varchar(255) NULL," +
                "industry varchar(255) NULL," +
                "total_score DOUBLE NULL," +
                "label_level_two varchar(255) NULL," +
                "publish_time timestamp NULL," +
                "title varchar NULL," +
                "summary varchar(255) NULL," +
                "content varchar NULL," +
                "content_confirm varchar NULL," +
                "detail_url varchar NULL," +
                "site_name varchar(255) NULL," +
                "update_time timestamp NULL ," +

                "is_delete int NOT NULL ," +
                "label_level_one varchar(255) NULL," +
                "PRIMARY KEY (company_code) NOT ENFORCED " +
                ") WITH (" +
                "  'connector' = 'jdbc'," +
                "  'url' = 'jdbc:postgresql://192.168.200.69:5432/brain_saas?useUnicode=true&amp;characterEncoding=utf8&amp;currentSchema=ads'," +
                "  'table-name' = 'ads.ads_investment_signal'," +
                "  'username' = 'brain_saas'," +
                "  'password' = 'bjbsHt2oY)l2V40j'," +
                "  'scan.fetch-size' = '1000'" +
                ")";

        // 创建表
        tableEnv.executeSql(createTableSql);

        // 读取表数据
        Table resultTable = tableEnv.sqlQuery("SELECT * FROM ads_investment_signal");

        // 处理数据（这里仅打印）
        //tableEnv.toDataStream(resultTable).print();
        DataStream<Row> dataStream = tableEnv.toDataStream(resultTable);

        DataStream<InvestmentSignalVo> userDataStream = dataStream.map(new MapFunction<Row, InvestmentSignalVo>() {
            @Override
            public InvestmentSignalVo map(Row row) throws Exception {
                InvestmentSignalVo investmentSignalVo = new InvestmentSignalVo(row);
                return investmentSignalVo; // 调用  类的构造函数
            }
        });

        // 执行程序
        env.execute("Flink Postgres Example");
    }
}
