package com.cjw.elasticsearch.client.util;

import com.cjw.elasticsearch.common.RestClientHelper;
import com.cjw.elasticsearch.common.exception.EsOperationException;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.io.IOException;
import java.util.Map;

/**
 * @author Ego
 * @version 1.0
 * @date 2020/7/2 18:30
 * @since JDK1.8
 */
@Slf4j
public class SearchUtil {
    private static String documentType = "_doc";

    @Resource
    private RestClientHelper restClientHelper;
    private RestHighLevelClient client;

    @PostConstruct
    private void init() {
        client = restClientHelper.getClient();
    }

    /**
     * 查询所有
     * @param index
     * @throws IOException
     */
    public void searchAll(String index) throws IOException {
        try {
            SearchRequest searchRequestAll = new SearchRequest();
            searchRequestAll.indices(index);
            searchRequestAll.types(documentType);

            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());
            searchRequestAll.source(searchSourceBuilder);
            // 同步查询
            SearchResponse searchResponseAll = client.search(searchRequestAll);
            System.out.println("所有查询总数:" + searchResponseAll.getHits().getTotalHits());
        } catch (Exception e) {
            throw new EsOperationException(e);
        }

    }

    /**
     * 查询指定文档的数据
     * @param index
     * @param id
     * @param includeFields
     * @return
     */
    public Map<String, Object> searchTagsById(String index, String id, String[] includeFields, String[] excludeTags){
        log.info(">>>>>>>> Search Data By Id:{}", id);
        try {
            GetRequest getRequest = new GetRequest(index, documentType, id);
            getRequest.refresh(true);

            // 根据指定参数列表返回标签
            if (!StringUtils.isEmpty(includeFields) || !StringUtils.isEmpty(excludeTags)) {
                FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includeFields, excludeTags);
                getRequest.fetchSourceContext(fetchSourceContext);
            }

            GetResponse getResponse = client.get(getRequest);
            if (getResponse.isExists()) {
                Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
                return sourceAsMap;
            }
        } catch (Exception e) {
            throw new EsOperationException(e);
        }
        return null;
    }

}
