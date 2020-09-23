package com.cjw.elasticsearch.bboss.domain;

import com.frameworkset.orm.annotation.ESId;
import lombok.Data;
import org.frameworkset.elasticsearch.entity.ESBaseData;

import java.io.Serializable;

/**
 * @author Ego
 * @version 1.0
 * @date 2020/6/29 14:24
 * @since JDK1.8
 */
@Data
public class User extends ESBaseData implements Serializable {

    /**
     * 主键
     */
    @ESId
    private Long cid;

    /**
     * 客户姓名
     */
    private String name;

    /**
     * 客户性别 对应字典表中COM.GENDER
     */
    private String gender;

    /**
     * 客户年龄
     */
    private Integer age;

}
