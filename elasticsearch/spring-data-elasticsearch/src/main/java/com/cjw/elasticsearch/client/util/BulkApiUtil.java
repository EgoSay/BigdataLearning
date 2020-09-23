package com.cjw.elasticsearch.client.util;

import com.alibaba.fastjson.JSON;
import com.cjw.elasticsearch.common.RestClientHelper;
import com.cjw.elasticsearch.common.exception.EsOperationException;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.HashMap;
import java.util.List;
import java.util.function.BiConsumer;

/**
 * @author Ego
 * @version 1.0
 * @date 2020/7/2 18:29
 * @since JDK1.8
 */
@Slf4j
public class BulkApiUtil {
    private static String documentType = "_doc";
    private static int bulkSize = 10000;

    @Resource
    private RestClientHelper restClientHelper;
    private RestHighLevelClient client;

    @PostConstruct
    private void init() {
        client = restClientHelper.getClient();
    }

    /**
     * bulk 批量插入
     * @param dataList
     */
    public void bulkInsertEs(List<HashMap<String, String>> dataList) {
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulkConsumer =
                (request, bulkListener) ->
                        client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener);
        BulkProcessor bulkProcessor = BulkProcessor.builder(bulkConsumer, new BulkProcessor.Listener() {
            //调用Bulk之前执行，例如可以通过request.numberOfActions()方法知道numberOfActions
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                log.info(">>>>>>>>>>> Bulk Insert Data to Es, the Data Count:{}", request.numberOfActions());
            }

            // 调用Bulk之后执行，例如可以通过response.hasFailures()方法知道是否执行失败
            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                if (response.hasFailures()) {
                    log.error(">>>>>>>>>>>> Bulk Insert is Failure, the failure message:" + response.buildFailureMessage());
                }
            }

            // 调用失败抛出异常
            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                throw new EsOperationException(failure);
            }
        }).setBulkActions(bulkSize)
                /** 拆成5MB一块 */
                .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
                /** 无论请求数量多少，每5秒钟请求一次*/
                .setFlushInterval(TimeValue.timeValueSeconds(5))
                /** 设置并发请求的数量。值为0意味着只允许执行一个请求。值为1意味着允许1并发请求*/
                .setConcurrentRequests(1)
                /** 设置自定义重复请求机制，最开始等待100毫秒，之后成倍增加，重试3次，当一次或者多次重复请求失败后因为计算资源不够抛出EsRejectedExecutionException异常，可以通过BackoffPolicy.noBackoff()方法关闭重试机制*/
                .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                .build();


        if (!CollectionUtils.isEmpty(dataList)) {
            for (HashMap map : dataList) {
                String index = null;
                String id = null;
                if (map.containsKey("index")) {
                    index = map.get("index").toString();
                }
                if (map.containsKey("id")) {
                    id = map.get("id").toString();
                }

                if (index != null && id != null) {
                    // IndexRequest indexRequest = new IndexRequest(index, documentType, id);
                    // indexRequest.source(map.remove("index"));
                    UpdateRequest updateRequest = new UpdateRequest(index, documentType, id);
                    updateRequest.docAsUpsert(true);
                    updateRequest.retryOnConflict(3);
                    map.remove("index");
                    map.remove("id");
                    String json = JSON.toJSONString(map);
                    updateRequest.doc(json, XContentType.JSON);
                    bulkProcessor.add(updateRequest);
                }

            }
        }
        bulkProcessor.flush();
        bulkProcessor.close();

    }
}
