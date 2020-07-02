package com.cjw.elasticsearch.client.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cjw.elasticsearch.common.RestClientHelper;
import com.cjw.elasticsearch.common.exception.EsOperationException;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.Map;

/**
 *  Document API of official Java High Level Rest client
 * @author Ego
 * @version 1.0
 * @date 2020/6/16 11:14
 * @since JDK1.8
 */
@Slf4j
@Component
public class EsDocumentUtil {

    private static String documentType = "_doc";

    @Resource
    private RestClientHelper restClientHelper;
    private RestHighLevelClient client;

    @PostConstruct
    private void init() {
        client = restClientHelper.getClient();
    }

    /**
     * 更新或者新增数据
     * @param index 索引
     * @param id 文档ID
     * @param entity 属性Map
     */
    public void insertOrUpdateData(String index, String id, Map entity){
        log.info(">>>>>>>>>>>>>>>>>>> Insert data to ES, data:{}", JSONObject.toJSONString(entity));
        try {
            UpdateRequest updateRequest = new UpdateRequest(index, documentType, id);
            updateRequest.doc(JSON.toJSONString(entity), XContentType.JSON);
            // upsert 表示如果数据不存在，那么就新增一条
            updateRequest.docAsUpsert(true);
            updateRequest.retryOnConflict(3);
            // 更新后立即刷新
            updateRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            UpdateResponse response = client.update(updateRequest);
            if (response.status() == RestStatus.OK) {
                log.info(">>>>>>>>>>>>>>>>>>>  insert data into elasticsearch successfully ! ");
            }

        } catch (Exception e) {
            throw new EsOperationException(e);
        }
    }

}
