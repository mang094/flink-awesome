package com.hw.news.task.blog;

import com.alibaba.fastjson.JSON;
import com.hw.news.kafka.CommonKafkaSourceHelper;
import com.hw.news.kafka.SaaSKafkaSInkHelper;
import com.hw.news.task.blog.dto.BlogNewsDto;
import com.hw.news.task.blog.function.BlogFilterFunction;
import com.hw.news.task.blog.function.BlogMapFunction;
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

public class BlogNewsSample {
    private static final String kafka_source_name = "kafka-blog-news-source";
    private static final String kafka_group_name = "kafka-blog-news-group";
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        KafkaSource<String> kafkaSource = CommonKafkaSourceHelper.kafkaSource(
                kafka_group_name, CommonKafkaSourceHelper.TOPIC_COMPANY_NEWS, OffsetsInitializer.earliest()
        );
        SingleOutputStreamOperator<BlogNewsDto> newsDs = env.fromSource(
                    kafkaSource, WatermarkStrategy.noWatermarks(), kafka_source_name)
                .filter(new BlogFilterFunction())
                .map(new BlogMapFunction());
        newsDs.addSink(auditSink());
        newsDs.map(JSON::toJSONString).sinkTo(SaaSKafkaSInkHelper.kafkaSink());
        env.execute();
    }

    private static SinkFunction<BlogNewsDto> auditSink() {
        return JdbcSink.sink(
                "INSERT INTO ads.ads_news_audit_blog (uuid, profile_name, profile_photo, publish_time, publish_source, content, cover_pic, digest, url, company_name, company_code) " +
                        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (JdbcStatementBuilder<BlogNewsDto>) (statement, kafkaNewsDto) -> {
                    statement.setString(1, kafkaNewsDto.getUuid());
                    statement.setString(2, kafkaNewsDto.getProfileName());
                    statement.setString(3, kafkaNewsDto.getProfilePhoto());
                    statement.setTimestamp(4, kafkaNewsDto.getPublishTime());
                    statement.setString(5, kafkaNewsDto.getPublishSource());
                    statement.setString(6, kafkaNewsDto.getContent());
                    statement.setString(7, kafkaNewsDto.getPicture());
                    statement.setString(8, kafkaNewsDto.getDigest());
                    statement.setString(9, kafkaNewsDto.getUrl());
                    statement.setString(10, kafkaNewsDto.getCompanyName());
                    statement.setString(11, kafkaNewsDto.getCompanyCode());
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
