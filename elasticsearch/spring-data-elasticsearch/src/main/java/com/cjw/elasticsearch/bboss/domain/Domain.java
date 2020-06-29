package com.cjw.elasticsearch.bboss.domain;

import com.frameworkset.orm.annotation.ESId;
import org.frameworkset.elasticsearch.entity.ESBaseData;

import java.util.Date;

/**
 * @author Ego
 * @version 1.0
 * @date 2020/6/29 16:37
 * @since JDK1.8
 */
public class Domain extends ESBaseData {
    @ESId
    private Long demoId;
    private String contentbody;
    /**  当在mapping定义中指定了日期格式时，则需要指定以下两个注解,例如
     *
     "agentStarttime": {
     "type": "date",###指定多个日期格式
     "format":"yyyy-MM-dd HH:mm:ss.SSS||yyyy-MM-dd'T'HH:mm:ss.SSS||yyyy-MM-dd HH:mm:ss||epoch_millis"
     }
     @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss.SSS")
     @Column(dataformat = "yyyy-MM-dd HH:mm:ss.SSS")
     */

    protected Date agentStarttime;
    private String applicationName;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    private String name;

    public String getContentbody() {
        return contentbody;
    }

    public void setContentbody(String contentbody) {
        this.contentbody = contentbody;
    }

    public Date getAgentStarttime() {
        return agentStarttime;
    }

    public void setAgentStarttime(Date agentStarttime) {
        this.agentStarttime = agentStarttime;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public void setApplicationName(String applicationName) {
        this.applicationName = applicationName;
    }

    public Long getDemoId() {
        return demoId;
    }

    public void setDemoId(Long demoId) {
        this.demoId = demoId;
    }
}
