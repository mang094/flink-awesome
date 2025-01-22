//package com.hw.connectors.elasticsearch;
//
//import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
//import org.apache.flink.connector.elasticsearch.sink.ElasticsearchSink;
//import org.apache.http.HttpHost;
//import org.elasticsearch.action.index.IndexRequest;
//import org.elasticsearch.client.Requests;
//
//import java.util.List;
//import java.util.function.Function;
//
//public class ElasticsearchConnectorUtil {
//
//    public static <T> ElasticsearchSink<T> createElasticsearchSink(
//            List<HttpHost> httpHosts,
//            String index,
//            Function<T, IndexRequest> indexRequestBuilder) {
//
//        return new Elasticsearch7SinkBuilder<T>()
//                .setHosts(httpHosts.toArray(new HttpHost[0]))
//                .setEmitter((element, context, indexer) ->
//                        indexer.add(indexRequestBuilder.apply(element)))
//                .setBulkFlushMaxActions(1000)
//                .setBulkFlushMaxSizeMb(5)
//                .setBulkFlushInterval(1000)
//                .build();
//    }
//}