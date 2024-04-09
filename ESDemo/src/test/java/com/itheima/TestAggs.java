package com.itheima;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.naming.directory.SearchResult;
import java.io.IOException;
import java.util.List;

@SpringBootTest
@RunWith(SpringRunner.class)
public class TestAggs {


    @Autowired
    RestHighLevelClient client;

    //需求一：按照颜色分组，计算每个颜色卖出的个数
    @Test
    public void testAggs() throws IOException {
        // GET /tvs/_search
        // {
        //     "size": 0,
        //     "query": {"match_all": {}},
        //     "aggs": {
        //       "group_by_color": {
        //         "terms": {
        //             "field": "color"
        //         }
        //     }
        // }
        // }

        //1 构建请求
        SearchRequest searchRequest=new SearchRequest("tvs");

        //请求体
        SearchSourceBuilder searchSourceBuilder=new SearchSourceBuilder();
        searchSourceBuilder.size(0);
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());

        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("group_by_color").field("color");
        searchSourceBuilder.aggregation(termsAggregationBuilder);

        //请求体放入请求头
        searchRequest.source(searchSourceBuilder);

        //2 执行
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        //3 获取结果
        //   "aggregations" : {
        //       "group_by_color" : {
        //           "doc_count_error_upper_bound" : 0,
        //           "sum_other_doc_count" : 0,
        //            "buckets" : [
        //           {
        //               "key" : "红色",
        //               "doc_count" : 4
        //           },
        //           {
        //               "key" : "绿色",
        //                   "doc_count" : 2
        //           },
        //           {
        //               "key" : "蓝色",
        //                   "doc_count" : 2
        //           }
        // ]
        //       }
        Aggregations aggregations = searchResponse.getAggregations();
        Terms group_by_color = aggregations.get("group_by_color");
        List<? extends Terms.Bucket> buckets = group_by_color.getBuckets();
        for (Terms.Bucket bucket : buckets) {
            String key = bucket.getKeyAsString();
            System.out.println("key:"+key);

            long docCount = bucket.getDocCount();
            System.out.println("docCount:"+docCount);

            System.out.println("=================================");
        }
    }

    //需求二：按照颜色分组，计算每个颜色卖出的个数，每个颜色卖出的平均价格
    public void testAggs2(){
//        GET /tvs/_search
//        {
//            "size" : 0,
//            "aggs": {
//              "group_by_color": {
//                "terms": {
//                    "field": "color"
//                },
//                "aggs": {
    //                "avg_price": {
    //                        "avg": {
    //                            "field": "price"
//                        }
//                    }
//                }
//            }
//        }
//        }

        //1、构建请求
        SearchRequest searchRequest = new SearchRequest("tvs");

        //请求体
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(0);
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());

        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("group_by_color").field("color");

        //terms聚合下填充一个子聚合
        AvgAggregationBuilder avgAggregationBuilder = AggregationBuilders.avg("avg_price").field("price");
        termsAggregationBuilder.subAggregation(avgAggregationBuilder);

        //将请求体放入请求头

        //2、执行





    }
}
