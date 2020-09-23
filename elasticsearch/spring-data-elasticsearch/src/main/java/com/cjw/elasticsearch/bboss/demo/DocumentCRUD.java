package com.cjw.elasticsearch.bboss.demo;

import com.cjw.elasticsearch.bboss.domain.User;
import com.cjw.elasticsearch.bboss.response.SearchResultResponse;
import org.frameworkset.elasticsearch.ElasticSearchException;
import org.frameworkset.elasticsearch.boot.BBossESStarter;
import org.frameworkset.elasticsearch.client.ClientInterface;
import org.frameworkset.elasticsearch.entity.ESDatas;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;

import javax.annotation.Resource;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * bboss ES开源库测试
 * @author Ego
 * @version 1.0
 * @date 2020/6/17 12:09
 * @since JDK1.8
 */
@Service
public class DocumentCRUD {
    private Logger log = LoggerFactory.getLogger(DocumentCRUD.class);

    @Resource
    private BBossESStarter bBossESStarter;
    /**
     * DSL config file path
     */
    private String path = "esmapper/demo.xml";
    /**
     * indice name
     */
    private String indice = "demo";

    public void dropAndCreateAndGetIndex() {
        ClientInterface client = bBossESStarter.getConfigRestClient(path);

        try {
            // To determine whether the indice demo exists
            boolean exist = client.existIndice(indice);

            // Create indice demo if the indice demo does not exists
            if (exist) {
                client.dropIndice(indice);

            }
            // Create indice demo
            client.createIndiceMapping(indice, "createDemoIndice");

            //Gets the newly created indice structure
            String demoIndice = client.getIndice("demo");
            log.info("after createIndice client.getIndice(\"demo\") response:" + demoIndice);
        } catch (ElasticSearchException e) {
            e.printStackTrace();
        }
    }

    private User buildUser(Long id) {
        User user = new User();
        user.setCid(id);
        user.setAge(id.intValue());
        user.setGender("female");
        user.setName("test"+id);
        return user;
    }

    public void addAndUpdateDocument() {
        ClientInterface client = bBossESStarter.getConfigRestClient(path);

        for (int i = 0; i < 10; i++) {
            User user = buildUser((long) i);
            // Add the document and force refresh
            String response = client.addDocument(indice, "demo", user,"refresh=true");
            log.debug(">>>>> Print the result：addDocument: {}", response);
        }
        HashMap<String, String> map = new HashMap<>();
        map.put("update", "updated");
        client.updateDocument(indice, "demo", 2, map);

    }

    public void deleteDocument() {
        ClientInterface clientUtil = bBossESStarter.getRestClient();
        // Batch delete documents
        clientUtil.deleteDocuments(indice, "demo", new String[]{"2","3"});
    }

    /**
     * Use slice parallel scoll query all documents of indice demo by 2 thread tasks. DEFAULT_FETCHSIZE is 5000
     */
    public void searchAlParallel() {
        ClientInterface clientUtil = bBossESStarter.getRestClient();
        clientUtil.searchAllParallel(indice, User.class, 2);
    }

    /**
     * Search the documents
     */
    public SearchResultResponse search(String searchDSL) {
        ClientInterface clientUtil = bBossESStarter.getRestClient();
        Map<String,Object> params = new HashMap<String,Object>();
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            params.put("startTime",dateFormat.parse("2019-09-02 00:00:00").getTime());
        } catch (ParseException e) {
            e.printStackTrace();
        }
        params.put("endTime",System.currentTimeMillis());
        ESDatas<User> esDataList = clientUtil.searchList(indice + "/_search", searchDSL, params, User.class);
        if (ObjectUtils.isEmpty(esDataList)) {
            return null;
        }
        List<User> userList = esDataList.getDatas();

        SearchResultResponse searchResultResponse = new SearchResultResponse();
        searchResultResponse.setDataList(userList);
        searchResultResponse.setTotalSize(esDataList.getTotalSize());

        return searchResultResponse;
    }
}
