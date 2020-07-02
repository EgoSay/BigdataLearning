package com.cjw.elasticsearch.common;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/**
 * @author Ego
 * @version 1.0
 * @date 2020/6/16 11:39
 * @since JDK1.8
 */
@Slf4j
@Component
public class RestClientHelper {

    @Value("${spring.elasticsearch.bboss.elasticsearch.rest.hostNames}")
    private String clusterNodes;

    private volatile static RestHighLevelClient client;

    public  RestHighLevelClient getClient() {
        try {
            if (client == null) {
                synchronized (RestClientHelper.class) {
                    if (client == null) {
                        if (!StringUtils.isEmpty(clusterNodes)) {
                            String[] nodes = clusterNodes.split(",");
                            HttpHost[] hosts = new HttpHost[nodes.length];

                            for (int i = 0; i < nodes.length; i++) {
                                String[] nodeInfo = nodes[i].split(":");
                                HttpHost httpHost = new HttpHost(nodeInfo[0], Integer.parseInt(nodeInfo[1]));
                                hosts[i] = httpHost;
                            }

                            client = new RestHighLevelClient(RestClient.builder(hosts));
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("RestClientHelper Get RestHighLevelClient with failure: " + e.getMessage());
        }
        return client;
    }
}
