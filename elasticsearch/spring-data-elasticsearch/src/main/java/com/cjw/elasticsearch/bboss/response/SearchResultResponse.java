package com.cjw.elasticsearch.bboss.response;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * @author Ego
 * @version 1.0
 * @date 2020/6/29 15:12
 * @since JDK1.8
 */
@Data
public class SearchResultResponse implements Serializable {

    /**
     * 查询结果对象集合
     */
    private List dataList;

    /**
     * 查询结果条数
     */
    private Long totalSize;
}
