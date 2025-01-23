//package com.hw.data.job;
//
//import com.hw.entity.CombinedData;
//import com.hw.entity.MainTableData;
//import com.hw.entity.SubTableData;
//import com.ververica.cdc.connectors.mysql.source.MySqlSource;
//import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.state.ValueState;
//import org.apache.flink.api.common.state.ValueStateDescriptor;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
//import org.apache.flink.util.Collector;
//import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
//import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
//import org.apache.flink.connector.jdbc.JdbcSink;
//
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import com.alibaba.fastjson.JSON;
//import com.alibaba.fastjson.JSONObject;
//import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
//import org.apache.flink.util.Collector;
//import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
//import com.ververica.cdc.connectors.mysql.table.StartupOptions;
//import org.apache.kafka.connect.source.SourceRecord;
//import org.apache.kafka.connect.data.Struct;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.apache.log4j.BasicConfigurator;
//
//import java.util.Properties;
//
///**
// * 满足先更新主表，副表可以做补充
// * 满足字段更新，宽表也会更新
// *
// * 从 Flink 1.18 开始，guava 版本从 30 升级到 31（使用 31.1-jre-17.0）
// *  从 CDC 3.0.1 开始，guava 版本也从 30 升级到 31（使用 31.1-jre-17.0）
// *  因此 CDC 3.0.1 与 Flink 1.18 兼容，CDC 2.x 与 Flink 1.13-1.17 兼容。CDC 2.x 与 Flink 1.18 依赖冲突，CDC 3.0.1 与 Flink 1.17（或更早版本）依赖冲突。
// */
//
//public class DataStreamSyncWithoutWait {
//    private static final Logger LOG = LoggerFactory.getLogger(DataStreamSyncWithoutWait.class);
//
//    public static void main(String[] args) throws Exception {
//        // 初始化log4j配置
//        //BasicConfigurator.configure();
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        // 设置检查点,确保数据一致性
//        env.enableCheckpointing(5000);
//        // 设置并行度为1，避免数据分散
//        env.setParallelism(1);
//
//        Properties debeziumProperties = new Properties();
//        debeziumProperties.put("decimal.handling.mode", "string");
//        debeziumProperties.put("include.schema.changes", "false");
//        // 修改为initial以确保不丢失数据
//        debeziumProperties.put("snapshot.mode", "initial");
//
//        MySqlSource<MainTableData> mainSource = MySqlSource.<MainTableData>builder()
//            .hostname("localhost")
//            .port(3306)
//            .databaseList("test")
//            .tableList("test.main_table")
//            .username("root")
//            .password("123456")
//            .deserializer(new CustomJsonDebeziumDeserializationSchema<>(MainTableData.class))
//            .startupOptions(StartupOptions.initial()) // 修改为initial
//            .debeziumProperties(debeziumProperties)
//            .build();
//
//        MySqlSource<SubTableData> subSource = MySqlSource.<SubTableData>builder()
//            .hostname("localhost")
//            .port(3306)
//            .databaseList("test")
//            .tableList("test.sub_table")
//            .username("root")
//            .password("123456")
//            .deserializer(new CustomJsonDebeziumDeserializationSchema<>(SubTableData.class))
//            .startupOptions(StartupOptions.initial()) // 修改为initial
//            .debeziumProperties(debeziumProperties)
//            .build();
//
//        DataStream<MainTableData> mainStream = env.fromSource(mainSource,
//            WatermarkStrategy.noWatermarks(), "Main Source");
//        DataStream<SubTableData> subStream = env.fromSource(subSource,
//            WatermarkStrategy.noWatermarks(), "Sub Source");
//
//        mainStream = mainStream.map(data -> {
//            LOG.info("主表数据: {}", data);
//            return data;
//        }).name("主表数据");
//
//        subStream = subStream.map(data -> {
//            LOG.info("副表数据: {}", data);
//            return data;
//        }).name("副表数据");
//
//        DataStream<CombinedData> combinedStream = mainStream
//            .keyBy(data -> data.getId())
//            .connect(subStream.keyBy(data -> data.getMainTableId()))
//            .flatMap(new EnrichmentFunction())
//            .name("数据合并处理");
//
//        combinedStream
//            .map(combined -> {
//                LOG.info("准备写入数据: {}", combined);
//                return combined;
//            })
//            .addSink(JdbcSink.sink(
//                "INSERT INTO test.target_table (id, main_data, sub_data, timestamp) VALUES (?, ?, ?, ?) " +
//                "ON DUPLICATE KEY UPDATE main_data=?, sub_data=COALESCE(?, sub_data), timestamp=?",
//                (statement, combined) -> {
//                    try {
//                        statement.setLong(1, combined.getId());
//                        statement.setString(2, combined.getMainData());
//                        statement.setString(3, combined.getSubData());
//                        statement.setDate(4, combined.getTimestamp());
//                        statement.setString(5, combined.getMainData());
//                        statement.setString(6, combined.getSubData());
//                        statement.setDate(7, combined.getTimestamp());
//                        LOG.info("SQL参数设置完成: id={}, main_data={}, sub_data={}, ts={}",
//                            combined.getId(), combined.getMainData(), combined.getSubData(), combined.getTimestamp());
//                    } catch (Exception e) {
//                        LOG.error("设置SQL参数时发生错误: ", e);
//                        throw e;
//                    }
//                },
//                JdbcExecutionOptions.builder()
//                    .withBatchSize(1)
//                    .withBatchIntervalMs(200) // 减少批处理间隔,加快数据写入
//                    .withMaxRetries(3)
//                    .build(),
//                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//                    .withUrl("jdbc:mysql://localhost:3306/test?useSSL=false")
//                    .withDriverName("com.mysql.cj.jdbc.Driver")
//                    .withUsername("root")
//                    .withPassword("123456")
//                    .build()
//            ))
//            .name("MySQL Sink");
//
//        env.execute("MySQL Sync Job");
//    }
//
//    private static class EnrichmentFunction
//            extends RichCoFlatMapFunction<MainTableData, SubTableData, CombinedData> {
//        private static final Logger LOG = LoggerFactory.getLogger(EnrichmentFunction.class);
//        private ValueState<MainTableData> mainState;
//        private ValueState<SubTableData> subState;
//
//        @Override
//        public void open(Configuration config) {
//            mainState = getRuntimeContext().getState(
//                new ValueStateDescriptor<>("main", MainTableData.class));
//            subState = getRuntimeContext().getState(
//                new ValueStateDescriptor<>("sub", SubTableData.class));
//        }
//
//        @Override
//        public void flatMap1(MainTableData main, Collector<CombinedData> out) throws Exception {
//            LOG.info("收到主表数据: id={}", main.getId());
//            mainState.update(main);
//            SubTableData sub = subState.value();
//            // 只更新主表数据,保留现有的副表数据
//            emit(main, sub, out);
//        }
//
//        @Override
//        public void flatMap2(SubTableData sub, Collector<CombinedData> out) throws Exception {
//            LOG.info("收到副表数据: mainTableId={}", sub.getMainTableId());
//            subState.update(sub);
//            MainTableData main = mainState.value();
//            if (main != null) {
//                // 更新合并后的数据
//                emit(main, sub, out);
//            }
//        }
//
//        private void emit(MainTableData main, SubTableData sub, Collector<CombinedData> out) {
//            try {
//                CombinedData combined = new CombinedData();
//                combined.setId(main.getId());
//                combined.setMainData(main.getMainData());
//                combined.setSubData(sub != null ? sub.getSubData() : null);
//                combined.setTimestamp(main.getTimestamp());
//                LOG.info("发送合并数据: {}", combined);
//                out.collect(combined);
//            } catch (Exception e) {
//                LOG.error("合并数据时发生错误: ", e);
//                throw e;
//            }
//        }
//    }
//
//    public static class CustomJsonDebeziumDeserializationSchema<T> implements DebeziumDeserializationSchema<T> {
//        private static final Logger LOG = LoggerFactory.getLogger(CustomJsonDebeziumDeserializationSchema.class);
//        private final Class<T> clazz;
//
//        public CustomJsonDebeziumDeserializationSchema(Class<T> clazz) {
//            this.clazz = clazz;
//        }
//
//        @Override
//        public void deserialize(SourceRecord sourceRecord, Collector<T> collector) throws Exception {
//            try {
//                Struct value = (Struct) sourceRecord.value();
//                Struct after = value.getStruct("after");
//                if (after != null) {
//                    JSONObject jsonObject = new JSONObject();
//                    after.schema().fields().forEach(field -> {
//                        Object fieldValue = after.get(field);
//                        jsonObject.put(field.name(), fieldValue);
//                    });
//
//                    T data = JSON.parseObject(jsonObject.toJSONString(), clazz);
//                    LOG.debug("反序列化数据: {}", data);
//                    collector.collect(data);
//                }
//            } catch (Exception e) {
//                LOG.error("反序列化数据时发生错误: ", e);
//                throw e;
//            }
//        }
//
//        @Override
//        public TypeInformation<T> getProducedType() {
//            return TypeInformation.of(clazz);
//        }
//    }
//}