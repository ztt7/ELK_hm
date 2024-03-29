package com.itheima;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;

@SpringBootTest(classes = SearchApplication.class)
@RunWith(SpringRunner.class)
public class TestIndex {

    @Autowired
    RestHighLevelClient client;

    @Test
    public void testCreateIndex() throws IOException {
        //创建索引对象
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("my_index_create");
        //设置参数
        createIndexRequest.settings(Settings.builder().put("number_of_shards","1").put("number_of_replicas","1").build());
        //指定映射1
        createIndexRequest.mapping(" {\n" +
                " \t\"properties\": {\n" +
                "            \"name\":{\n" +
                "             \"type\":\"keyword\"\n" +
                "           },\n" +
                "           \"description\": {\n" +
                "              \"type\": \"text\"\n" +
                "           },\n" +
                "            \"price\":{\n" +
                "             \"type\":\"long\"\n" +
                "           },\n" +
                "           \"pic\":{\n" +
                "             \"type\":\"text\",\n" +
                "             \"index\":false\n" +
                "           }\n" +
                " \t}\n" +
                "}", XContentType.JSON);

        //指定映射2
//        Map<String, Object> message = new HashMap<>();
//        message.put("type", "text");
//        Map<String, Object> properties = new HashMap<>();
//        properties.put("message", message);
//        Map<String, Object> mapping = new HashMap<>();
//        mapping.put("properties", properties);
//        createIndexRequest.mapping(mapping);


        //指定映射3
//        XContentBuilder builder = XContentFactory.jsonBuilder();
//        builder.startObject();
//        {
//            builder.startObject("properties");
//            {
//                builder.startObject("message");
//                {
//                    builder.field("type", "text");
//                }
//                builder.endObject();
//            }
//            builder.endObject();
//        }
//        builder.endObject();
//        createIndexRequest.mapping(builder);


        //设置别名
        createIndexRequest.alias(new Alias("itheima_index_new"));

        //额外参数
        //设置超时时间
        createIndexRequest.setTimeout(TimeValue.timeValueMinutes(2));
        //设置主节点超时时间
        createIndexRequest.setMasterTimeout(TimeValue.timeValueMinutes(1));
        //在创建索引api返回响应之前等待的活动分片副本的数量，以int形式表示
        createIndexRequest.waitForActiveShards(ActiveShardCount.from(2));
        createIndexRequest.waitForActiveShards(ActiveShardCount.DEFAULT);

        //操作索引的客户端
        IndicesClient indices = client.indices();
        //执行创建索引库（这是同步方式）
        CreateIndexResponse createIndexResponse = indices.create(createIndexRequest, RequestOptions.DEFAULT);
        //得到相应（全部）
        boolean acknowledged = createIndexResponse.isAcknowledged();
        //得到响应 指示是否在超时前为索引中的每个分片启动了所需数量的碎片副本
        boolean shardsAcknowledged = createIndexResponse.isShardsAcknowledged();

        System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!" + acknowledged);
        System.out.println(shardsAcknowledged);

    }

    //异步新增索引
    @Test
    public void testCreateIndexAsync() throws IOException {
        //创建索引对象
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("my_index_createAs");
        //设置参数
        createIndexRequest.settings(Settings.builder().put("number_of_shards","1").put("number_of_replicas","1").build());
        //指定映射3
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("properties");
            {
                builder.startObject("message");
                {
                    builder.field("type", "text");
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
        createIndexRequest.mapping(builder);
        //设置别名
        createIndexRequest.alias(new Alias("index_1"));

        //额外参数
//        //设置超时时间
//        createIndexRequest.setTimeout(TimeValue.timeValueMinutes(2));
//        //设置主节点超时时间
//        createIndexRequest.setMasterTimeout(TimeValue.timeValueMinutes(1));
//        //在创建索引api返回响应之前等待的活动分片副本的数量，以int形式表示
//        createIndexRequest.waitForActiveShards(ActiveShardCount.from(2));
//        createIndexRequest.waitForActiveShards(ActiveShardCount.DEFAULT);

        //监听方法
        ActionListener<CreateIndexResponse> listener = new ActionListener<CreateIndexResponse>() {
            @Override
            public void onResponse(CreateIndexResponse createIndexResponse) { //得到相应（全部）
                boolean acknowledged = createIndexResponse.isAcknowledged();
                //得到响应 指示是否在超时前为索引中的每个分片启动了所需数量的碎片副本
                boolean shardsAcknowledged = createIndexResponse.isShardsAcknowledged();

                System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!" + acknowledged);
                System.out.println(shardsAcknowledged);

                System.out.println("索引创建成功！！！");
                System.out.println(createIndexResponse.toString());
            }

            @Override
            public void onFailure(Exception e) {
                System.out.println("索引创建失败！！！");
                e.printStackTrace();
            }
        };

        //操作索引的客户端
        IndicesClient indices = client.indices();
        //执行创建索引库
        indices.createAsync(createIndexRequest,RequestOptions.DEFAULT,listener);

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }



}
