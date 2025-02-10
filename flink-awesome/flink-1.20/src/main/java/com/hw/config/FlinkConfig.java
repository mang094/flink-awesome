package com.hw.config;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkConfig {
    public static StreamExecutionEnvironment getEnv() {
        Configuration configuration = new Configuration();
        // 设置checkpoint间隔为30秒
        configuration.setString("execution.checkpointing.interval", "30s");
        // 设置状态后端为RocksDB
        configuration.setString("state.backend", "rocksdb");
        // 设置增量checkpoint
        configuration.setString("state.backend.incremental", "true");
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        // 设置并行度
        env.setParallelism(4);
        return env;
    }

    public static StreamTableEnvironment getTableEnv(StreamExecutionEnvironment env) {
        return StreamTableEnvironment.create(env);
    }
} 