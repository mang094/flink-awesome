package com.hw.connectors.elasticsearch;

import com.hw.data.model.UserInfo;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;

public class ElasticsearchHttpSink implements SinkFunction<UserInfo> {
    @Override
    public void invoke(UserInfo value, Context context) throws Exception {
        // 示例：使用 HTTP 客户端发送请求到 Elasticsearch
        //时间插入目前有问题
        String json = String.format(
                "{\"id\": %d, \"name\": \"%s\", \"age\": %d, \"email\": \"%s\", \"update_time\": \"%s\"}",
                value.getId(), value.getName(), value.getAge(), value.getEmail(), value.getUpdateTime());
        HttpPost post = new HttpPost("http://localhost:9200/user_info/_doc/" + value.getId());
        post.setEntity(new StringEntity(json, ContentType.APPLICATION_JSON));
        HttpClient client = HttpClients.createDefault();
        client.execute(post);
    }
}
