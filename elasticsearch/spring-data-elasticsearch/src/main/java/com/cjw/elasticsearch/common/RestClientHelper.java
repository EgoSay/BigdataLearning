package com.cjw.elasticsearch.common;

import org.apache.http.HttpHost;
import org.apache.log4j.Logger;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

/**
 * @author Ego
 * @version 1.0
 * @date 2020/6/16 11:39
 * @since JDK1.8
 */
@Configuration
public class RestClientHelper {

    @Value("${elasticsearch.cluster-nodes}")
    private String clusterNodes;
    @Value("${elasticsearch.cluster-name}")
    private String clusterName;
    @Value("${elasticsearch.request-type}")
    private String requestType;

    private static final Logger log = Logger.getLogger(RestClientHelper.class);
    private volatile static RestHighLevelClient client;

    public RestClientHelper() {
        if (!StringUtils.isEmpty(clusterNodes)) {
            String[] nodes = clusterNodes.split(",");
            HttpHost[] hosts = new HttpHost[nodes.length];

            for (int i = 0; i < nodes.length; i++) {
                String[] nodeInfo = nodes[i].split(":");
                HttpHost httpHost = new HttpHost(nodeInfo[0], Integer.parseInt(nodeInfo[1]), requestType);
                hosts[i] = httpHost;
            }

            RestClientBuilder clientBuilder = RestClient.builder(hosts);
            client = new RestHighLevelClient(clientBuilder);
        }
    }


    public static RestHighLevelClient getClient() {
        try {
            if (client == null) {
                synchronized (RestClientHelper.class) {
                    if (client == null) {
                        RestClientHelper restClientHelper = new RestClientHelper();
                    }
                }
            }
        } catch (Exception e) {
            log.error("RestClientHelper Get RestHighLevelClient with failure: " + e.getMessage());
        }
        return client;
    }
}
