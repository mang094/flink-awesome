package com.hw.job;

import com.hw.config.FlinkConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class CatalogSyncJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkConfig.getEnv();
        StreamTableEnvironment tableEnv = FlinkConfig.getTableEnv(env);

        // 创建源表
        tableEnv.executeSql(
            "CREATE TABLE bring_avspare_catalog (" +
            "    id BIGINT," +
            "    brand_id BIGINT," + 
            "    catalog_name STRING," +
            "    catalog_image_id BIGINT," +
            "    parent_catalog_id BIGINT," +
            "    parent_catalog_name STRING," +
            "    catalog_level INT," +
            "    type INT," +
            "    catalog_is_last INT," +
            "    first_catalog_id BIGINT," +
            "    first_catalog_name STRING," +
            "    second_catalog_id BIGINT," +
            "    second_catalog_name STRING," +
            "    third_catalog_id BIGINT," +
            "    third_catalog_name STRING," +
            "    fourth_catalog_id BIGINT," +
            "    fourth_catalog_name STRING," +
            "    fifth_catalog_id BIGINT," +
            "    fifth_catalog_name STRING," +
            "    sixth_catalog_id BIGINT," +
            "    sixth_catalog_name STRING," +
            "    seventh_catalog_id BIGINT," +
            "    seventh_catalog_name STRING," +
            "    eighth_catalog_id BIGINT," +
            "    eighth_catalog_name STRING," +
            "    ninth_catalog_id BIGINT," +
            "    ninth_catalog_name STRING," +
            "    value_json STRING," +
            "    error_message STRING," +
            "    spider_status INT," +
            "    pull_count INT," +
            "    add_count INT," +
            "    created_time TIMESTAMP(3)," +
            "    updated_time TIMESTAMP(3)," +
            "    origin_url STRING," +
            "    PRIMARY KEY (id) NOT ENFORCED" +
            ") WITH (" +
            "    'connector' = 'mysql-cdc'," +
            "    'hostname' = '192.168.16.126'," +
            "    'port' = '3306'," +
            "    'username' = 'spider_test'," +
            "    'password' = 'IiCIoYInniSx6LLh!'," +
            "    'database-name' = 'hw_spider'," +
            "    'table-name' = 'bring_avspare_catalog'" +
            ")"
        );

        tableEnv.executeSql(
            "CREATE TABLE bring_avspare_catalog_image (" +
            "    id BIGINT," +
            "    image_url STRING," +
            "    PRIMARY KEY (id) NOT ENFORCED" +
            ") WITH (" +
            "    'connector' = 'mysql-cdc'," +
            "    'hostname' = '192.168.16.126'," +
            "    'port' = '3306'," +
            "    'username' = 'spider_test'," +
            "    'password' = 'IiCIoYInniSx6LLh!'," +
            "    'database-name' = 'hw_spider'," +
            "    'table-name' = 'bring_avspare_catalog_image'" +
            ")"
        );

        tableEnv.executeSql(
            "CREATE TABLE apb_catalog_image (" +
            "    id BIGINT," +
            "    oss_links STRING," +
            "    processed_oss_links STRING," + 
            "    pixel STRING," +
            "    origin_image STRING," +
            "    init_coordinate STRING," +
            "    fpb_oss_status INT," +
            "    deleted_status INT," +
            "    source_id BIGINT," +
            "    created_time TIMESTAMP(3)," +
            "    updated_time TIMESTAMP(3)," +
            "    PRIMARY KEY (id) NOT ENFORCED" +
            ") WITH (" +
            "    'connector' = 'mysql-cdc'," +
            "    'hostname' = '192.168.16.126'," +
            "    'port' = '3306'," +
            "    'username' = 'spider_test'," +
            "    'password' = 'IiCIoYInniSx6LLh!'," +
            "    'database-name' = 'hw_spider'," +
            "    'table-name' = 'apb_catalog_image'" +
            ")"
        );

        // 创建目标表
        tableEnv.executeSql(
            "CREATE TABLE avspare_case_apb_catalog_v1 (" +
            "    catalog_id BIGINT," +
            "    apb_brand_id BIGINT," + 
            "    apb_catalog_image_id BIGINT," +
            "    brand_name VARCHAR(255)," +
            "    catalog_name VARCHAR(255)," +
            "    type TINYINT," +
            "    parent_catalog_id BIGINT," +
            "    parent_catalog_name VARCHAR(255)," +
            "    catalog_level TINYINT," +
            "    last_type TINYINT," +
            "    source_id BIGINT," +
            "    created_time TIMESTAMP(3)," +
            "    updated_time TIMESTAMP(3)," +
            "    PRIMARY KEY (catalog_id) NOT ENFORCED" +
            ") WITH (" +
            "    'connector' = 'jdbc'," +
            "    'url' = 'jdbc:mysql://192.168.16.126:3306/hw_spider'," +
            "    'table-name' = 'avspare_case_apb_catalog_v1'," +
            "    'username' = 'spider_test'," +
            "    'password' = 'IiCIoYInniSx6LLh!'" +
            ")"
        );

        // 执行同步查询
        tableEnv.executeSql(
            "INSERT INTO avspare_case_apb_catalog_v1 " +
            "WITH image AS (" +
            "    SELECT " +
            "        skci.id as catalog_id," +
            "        aci.id as apb_catalog_image_id" +
            "    FROM bring_avspare_catalog_image skci" +
            "    JOIN apb_catalog_image aci" +
            "    ON skci.image_url = aci.origin_image" +
            ")" +
            "SELECT " +
            "    skc.id as catalog_id," +
            "    skc.brand_id as apb_brand_id," +
            "    image.apb_catalog_image_id," +
            "    CAST('Case IH' AS STRING) as brand_name," +
            "    skc.catalog_name," +
            "    CAST(CASE WHEN skc.parent_catalog_id = 0 THEN 0 ELSE skc.type END AS TINYINT) as type," +
            "    skc.parent_catalog_id," +
            "    skc.parent_catalog_name," +
            "    CAST(skc.catalog_level AS TINYINT) as catalog_level," +
            "    CAST(skc.catalog_is_last AS TINYINT) as last_type," +
            "    CAST(0 AS BIGINT) as source_id," +
            "    skc.created_time," +
            "    skc.updated_time" +
            " FROM bring_avspare_catalog skc" +
            " LEFT JOIN image ON image.catalog_id = skc.catalog_image_id"
        );

        env.execute("Catalog Sync Job");
    }
} 