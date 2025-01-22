package com.hw.news.task.test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static com.hsmap.news.jdbc.SaasPostgresHelper.*;

public class FlinkPgSQLExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(JDBC_URL_ADS)
                .withDriverName(JDBC_DRIVER_NAME)
                .withUsername(JDBC_USER_NAME)
                .withPassword(JDBC_USER_PASSWORD)
                .withConnectionCheckTimeoutSeconds(60)
                .build();
        //String query = "SELECT   *  from (select dmme.company_code, ci.company_name,ci.company_sname, cis.industry, dmme.total_score, dmme.label_one label_level_one,dmme.label_two label_level_two, dmme.date publish_time, news.title, news.summary, news.content, news.content_confirm, news.detail_url, news.site_name, greatest ( dmme.update_time, news.update_time) as update_time from ( select  event.id, event.company_code, event.date, event.event, event.label_one, event.label_two, event.score, event.dm_id, event.update_time, score.score total_score from ( select id, company_code, date, event, label_one, label_two, score, dm_id, update_time, row_number() over (partition by company_code order by date desc nulls last ) maxx from dm_modd.dm_migrate_mod_event where is_delete = 0) event join ( select company_code, score from dm_modd.dm_migrate_mod_score where is_delete = 0 and  label = '总分') score on event.company_code = score.company_code and event.maxx = 1 ) dmme left join public.dm_poif_news news on news.id = dmme.dm_id left join public.dm_lget_company_info ci on dmme.company_code = ci.company_code left join ( select company_code, string_agg(distinct level_first, ',') industry from public.dm_lget_company_industry_strategic where is_delete = 0 group by company_code ) cis on cis.company_code = dmme.company_code where cis.industry is not null)";
        String query = "SELECT   company_code  from ads.ads_investment_signal ";
        JdbcInputFormat jdbcInputFormat = JdbcInputFormat.buildJdbcInputFormat()
                .setDrivername(JDBC_DRIVER_NAME)
                .setDBUrl(JDBC_URL_ADS)
                .setUsername(JDBC_USER_NAME)
                .setPassword(JDBC_USER_PASSWORD)
                .setQuery(query)
                .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO))

                .finish();
        DataStreamSource<Row> input = env.createInput(jdbcInputFormat);


        DataStream<InvestmentSignalVo> userDataStream = input.map(new MapFunction<Row, InvestmentSignalVo>() {
            @Override
            public InvestmentSignalVo map(Row row) throws Exception {
                return new InvestmentSignalVo(row); // 调用  类的构造函数
            }
        });
        try {
            userDataStream.addSink(auditSink());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        // 现在你可以对用户数据流进行进一步的处理或输出了
        // userDataStream.print();

        env.execute("Flink JDBC PostgreSQL to Object Example");


    }

    private static SinkFunction<InvestmentSignalVo> auditSink() {
        return JdbcSink.sink(
                "INSERT INTO ads.ads_investment_signal_v10 (company_code) " +
                        " VALUES (?)",
                (JdbcStatementBuilder<InvestmentSignalVo>) (statement, vo) -> {
                    statement.setString(1, vo.getCompanyCode());

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
