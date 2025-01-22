package com.hw.news.task.realtime;

import com.hw.news.kafka.CommonKafkaSourceHelper;
import com.hw.news.task.realtime.dto.RealtimeNewsDto;
import com.hw.news.task.realtime.function.NewsFilterFunction;
import com.hw.news.task.realtime.function.NewsMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import static com.hw.news.jdbc.SaasPostgresHelper.*;

public class RealtimeNewsSample {
    private static final String kafka_source_name = "kafka-realtime-news-source";
    private static final String kafka_group_name = "kafka-realtime-news-group";
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        KafkaSource<String> kafkaSource = CommonKafkaSourceHelper.kafkaSource(
                kafka_group_name, CommonKafkaSourceHelper.TOPIC_NEWS, OffsetsInitializer.latest());
        SingleOutputStreamOperator<RealtimeNewsDto> newsDs = env.fromSource(
                    kafkaSource, WatermarkStrategy.noWatermarks(), kafka_source_name)
                .filter(new NewsFilterFunction())
                .map(new NewsMapFunction());
        try {
            newsDs.addSink(auditTblSink());
        } catch (Exception e) {
            e.printStackTrace();
        }
        //newsDs.map(JSON::toJSONString).sinkTo(SaaSKafkaSInkHelper.kafkaSink());
        env.execute();
    }

    private static SinkFunction<RealtimeNewsDto> auditSink() {
        return JdbcSink.sink(
                "INSERT INTO ads.ads_news_audit_realtime (uuid, title, content, detail_url, site_name, publish_time) " +
                        " VALUES (?, ?, ?, ?, ?, ?)",
                (JdbcStatementBuilder<RealtimeNewsDto>) (statement, kafkaNewsDto) -> {
                    statement.setString(1, kafkaNewsDto.getUuid());
                    statement.setString(2, kafkaNewsDto.getTitle());
                    statement.setString(3, kafkaNewsDto.getContent());
                    statement.setString(4, kafkaNewsDto.getDetailUrl());
                    statement.setString(5, kafkaNewsDto.getSiteName());
                    statement.setTimestamp(6, kafkaNewsDto.getPublishTime());
                },
                JdbcExecutionOptions.builder()
                        .withMaxRetries(3)
                        .withBatchSize(100)
                        .withBatchIntervalMs(3000)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(JDBC_URL_ADS)
                        .withDriverName(JDBC_DRIVER_NAME)
                        .withUsername(JDBC_USER_NAME)
                        .withPassword(JDBC_USER_PASSWORD)
                        .withConnectionCheckTimeoutSeconds(60)
                        .build()
        );
    }

    private static SinkFunction<RealtimeNewsDto> auditTblSink() {
        return JdbcSink.sink(
                "INSERT INTO public.tbl_news (title, content, detail_url, site_name, publish_time) " +
                        " VALUES ( ?, ?, ?, ?, ?)",
                (JdbcStatementBuilder<RealtimeNewsDto>) (statement, kafkaNewsDto) -> {
                    //statement.setString(1, kafkaNewsDto.getUuid());
                    statement.setString(1, kafkaNewsDto.getTitle());
                    statement.setString(2, kafkaNewsDto.getContent());
                    statement.setString(3, kafkaNewsDto.getDetailUrl());
                    statement.setString(4, kafkaNewsDto.getSiteName());
                    statement.setTimestamp(5, kafkaNewsDto.getPublishTime());
                },
                JdbcExecutionOptions.builder()
                        .withMaxRetries(3)
                        .withBatchSize(100)
                        .withBatchIntervalMs(3000)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(JDBC_URL_ADS)
                        .withDriverName(JDBC_DRIVER_NAME)
                        .withUsername(JDBC_USER_NAME)
                        .withPassword(JDBC_USER_PASSWORD)
                        .withConnectionCheckTimeoutSeconds(60)
                        .build()
        );
    }

}
