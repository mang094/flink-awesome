//package com.hw.data.job;
//
//import com.alibaba.fastjson.JSON;
//import com.alibaba.fastjson.JSONObject;
//import com.hw.entity.CombinedData;
//import com.hw.entity.MainTableData;
//import com.hw.entity.SubTableData;
//import com.ververica.cdc.connectors.mysql.source.MySqlSource;
//import com.ververica.cdc.connectors.mysql.table.StartupOptions;
//import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.state.ListState;
//import org.apache.flink.api.common.state.ListStateDescriptor;
//import org.apache.flink.api.common.state.ValueState;
//import org.apache.flink.api.common.state.ValueStateDescriptor;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
//import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
//import org.apache.flink.connector.jdbc.JdbcSink;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
//import org.apache.flink.util.Collector;
//import org.apache.kafka.connect.data.Struct;
//import org.apache.kafka.connect.source.SourceRecord;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.time.Duration;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Properties;
//import java.util.stream.Collectors;
//
///**
// * 1.在flink-data模块下帮我生成代码
// * 2.需求是做数据同步
// * 3.两张表拉宽成一张表
// * 4.根据时间进行新增或者更新 `timestamp`
// * 5.沿用现有的MainTableData 、SubTableData 等表，副表一对多情况合并成一条用逗号隔开
// * 在处理副表数据时：
// * 如果是已存在的数据则更新
// * 如果是新数据则添加到列表中
// * 每次更新后都会重新生成完整的关联数据
// * 在处理主表数据时：
// * 更新主表状态
// * 获取所有副表数据并合并成逗号分隔的字符串
// * 生成关联数据
// */
//
//public class DataStreamJoinJob {
//    private static final Logger LOG = LoggerFactory.getLogger(DataStreamJoinJob.class);
//
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//
//        Properties debeziumProperties = new Properties();
//        debeziumProperties.put("decimal.handling.mode", "string");
//        debeziumProperties.put("include.schema.changes", "false");
//        debeziumProperties.put("snapshot.mode", "initial");
//
//        // 创建主表流
//        MySqlSource<MainTableData> mainSource = MySqlSource.<MainTableData>builder()
//                .hostname("localhost")
//                .port(3306)
//                .databaseList("test")
//                .tableList("test.main_table")
//                .username("root")
//                .password("123456")
//                .deserializer(new CustomJsonDebeziumDeserializationSchema<>(MainTableData.class))
//                .startupOptions(StartupOptions.initial())
//                .debeziumProperties(debeziumProperties)
//                .build();
//
//        // 创建副表流
//        MySqlSource<SubTableData> subSource = MySqlSource.<SubTableData>builder()
//                .hostname("localhost")
//                .port(3306)
//                .databaseList("test")
//                .tableList("test.sub_table")
//                .username("root")
//                .password("123456")
//                .deserializer(new CustomJsonDebeziumDeserializationSchema<>(SubTableData.class))
//                .startupOptions(StartupOptions.initial())
//                .debeziumProperties(debeziumProperties)
//                .build();
//
//        DataStream<MainTableData> mainStream = env.fromSource(mainSource,
//                WatermarkStrategy.<MainTableData>forBoundedOutOfOrderness(Duration.ofSeconds(3))
//                    .withTimestampAssigner((event, timestamp) -> event.getTimestamp().getTime()),
//                "Main Source");
//
//        DataStream<SubTableData> subStream = env.fromSource(subSource,
//                WatermarkStrategy.<SubTableData>forBoundedOutOfOrderness(Duration.ofSeconds(3))
//                    .withTimestampAssigner((event, timestamp) -> event.getTimestamp().getTime()),
//                "Sub Source");
//
//        // 进行数据关联
//        mainStream.connect(subStream)
//            .keyBy(MainTableData::getId, SubTableData::getMainTableId)
//            .process(new JoinProcessor())
//            .addSink(JdbcSink.sink(
//                    "INSERT INTO test.target_table (id, main_data, sub_data, timestamp) VALUES (?, ?, ?, ?) " +
//                            "ON DUPLICATE KEY UPDATE main_data=?, sub_data=?, timestamp=?",
//                    (statement, combined) -> {
//                        statement.setLong(1, combined.getId());
//                        statement.setString(2, combined.getMainData());
//                        statement.setString(3, combined.getSubData());
//                        statement.setDate(4, combined.getTimestamp());
//                        statement.setString(5, combined.getMainData());
//                        statement.setString(6, combined.getSubData());
//                        statement.setDate(7, combined.getTimestamp());
//                    },
//                    JdbcExecutionOptions.builder()
//                            .withBatchSize(1)
//                            .withBatchIntervalMs(200)
//                            .withMaxRetries(3)
//                            .build(),
//                    new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//                            .withUrl("jdbc:mysql://localhost:3306/test?useSSL=false")
//                            .withDriverName("com.mysql.cj.jdbc.Driver")
//                            .withUsername("root")
//                            .withPassword("123456")
//                            .build()
//            ));
//
//        env.execute("Data Stream Join Job");
//    }
//
//    private static class JoinProcessor extends KeyedCoProcessFunction<Long, MainTableData, SubTableData, CombinedData> {
//        private ValueState<MainTableData> mainState;
//        private ListState<SubTableData> subListState;
//
//        @Override
//        public void open(Configuration parameters) {
//            // 初始化状态
//            mainState = getRuntimeContext().getState(
//                new ValueStateDescriptor<>("main-state", MainTableData.class));
//            subListState = getRuntimeContext().getListState(
//                new ListStateDescriptor<>("sub-list-state", SubTableData.class));
//        }
//
//        @Override
//        public void processElement1(MainTableData main, Context ctx, Collector<CombinedData> out) throws Exception {
//            // 处理主表数据
//            mainState.update(main);
//
//            // 获取副表数据列表
//            List<SubTableData> subList = new ArrayList<>();
//            subListState.get().forEach(subList::add);
//
//            if (!subList.isEmpty()) {
//                // 合并副表数据为逗号分隔的字符串
//                String subDataStr = subList.stream()
//                    .map(SubTableData::getSubData)
//                    .collect(Collectors.joining(","));
//
//                // 输出关联结果
//                CombinedData combined = new CombinedData();
//                combined.setId(main.getId());
//                combined.setMainData(main.getMainData());
//                combined.setSubData(subDataStr);
//                combined.setTimestamp(main.getTimestamp());
//                out.collect(combined);
//            }
//        }
//
//        @Override
//        public void processElement2(SubTableData sub, Context ctx, Collector<CombinedData> out) throws Exception {
//            // 处理副表数据
//            List<SubTableData> currentList = new ArrayList<>();
//            subListState.get().forEach(currentList::add);
//
//            // 检查是否存在相同数据，存在则更新，不存在则添加
//            boolean updated = false;
//            for (int i = 0; i < currentList.size(); i++) {
//                if (currentList.get(i).getSubData().equals(sub.getSubData())) {
//                    currentList.set(i, sub);
//                    updated = true;
//                    break;
//                }
//            }
//
//            if (!updated) {
//                currentList.add(sub);
//            }
//
//            // 更新状态
//            subListState.clear();
//            currentList.forEach(data -> {
//                try {
//                    subListState.add(data);
//                } catch (Exception e) {
//                    LOG.error("Error adding data to state", e);
//                }
//            });
//
//            // 如果主表数据存在，则生成关联数据
//            MainTableData main = mainState.value();
//            if (main != null) {
//                String subDataStr = currentList.stream()
//                    .map(SubTableData::getSubData)
//                    .collect(Collectors.joining(","));
//
//                CombinedData combined = new CombinedData();
//                combined.setId(main.getId());
//                combined.setMainData(main.getMainData());
//                combined.setSubData(subDataStr);
//                combined.setTimestamp(main.getTimestamp());
//                out.collect(combined);
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