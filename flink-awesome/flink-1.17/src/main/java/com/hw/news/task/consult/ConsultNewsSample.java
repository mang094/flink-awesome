package com.hw.news.task.consult;

import com.alibaba.fastjson.JSON;
import com.hw.news.kafka.CommonKafkaSourceHelper;
import com.hw.news.kafka.SaaSKafkaSInkHelper;
import com.hw.news.task.consult.dto.ConsultNewsDto;
import com.hw.news.task.consult.function.ConsultNewsMapFunction;
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
import static com.hw.news.jdbc.SaasPostgresHelper.JDBC_USER_PASSWORD;

public class ConsultNewsSample {
    private static final String kafka_source_name = "kafka-consult-news-source";
    private static final String kafka_group_name = "kafka-consult-news-group";
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        KafkaSource<String> kafkaSource = CommonKafkaSourceHelper.kafkaSource(
                kafka_group_name, CommonKafkaSourceHelper.TOPIC_CONSULT_NEWS, OffsetsInitializer.earliest()
        );
        SingleOutputStreamOperator<ConsultNewsDto> newsDs =
                    env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), kafka_source_name)
                .map(new ConsultNewsMapFunction());
        newsDs.addSink(auditSink());
        newsDs.map(JSON::toJSONString).sinkTo(SaaSKafkaSInkHelper.kafkaSink());
        env.execute();
    }

    private static SinkFunction<ConsultNewsDto> auditSink() {

        return JdbcSink.sink(
                "INSERT INTO ads.ads_news_audit_consult (uuid, head_line, publish_time, publish_site, content, cover_pic, digest, url, dus_tag) " +
                        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (JdbcStatementBuilder<ConsultNewsDto>) (statement, kafkaNewsDto) -> {
                    statement.setString(1, kafkaNewsDto.getUuid());
                    statement.setString(2, kafkaNewsDto.getHeadline());
                    statement.setTimestamp(3, kafkaNewsDto.getPublishTime());
                    statement.setString(4, kafkaNewsDto.getPublishSite());
                    statement.setString(5, kafkaNewsDto.getContent());
                    statement.setString(6, kafkaNewsDto.getCoverPic());
                    statement.setString(7, kafkaNewsDto.getDigest());
                    statement.setString(8, kafkaNewsDto.getUrl());
                    statement.setString(9, kafkaNewsDto.getIndustryLabel());
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
