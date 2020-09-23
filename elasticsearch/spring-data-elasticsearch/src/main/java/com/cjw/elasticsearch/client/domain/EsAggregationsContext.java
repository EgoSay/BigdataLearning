package com.cjw.elasticsearch.client.domain;

import lombok.Data;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;

import java.util.Map;

/**
 * Es Aggregations 聚合统计查询参数
 * @author Ego
 * @version 1.0
 * @date 2020/9/23 14:24
 * @since JDK1.8
 */
@Data
public class EsAggregationsContext {

    /**
     * 需要进行分组的字段
     */
    private String field;

    /**
     * 索引
     */
    private String[] indices;

    /**
     * 数值型标签区间
     */
    private Double interval;

    /**
     * 初始偏移值
     */
    private double offset;

    /**
     * 时间区间， 针对时间型标签统计
     */
    private DateHistogramInterval dateHistogramInterval;

    /**
     * 返回时间格式
     */
    private String dateFormat;

    /**
     * 分类型标签分类字段和其对应
     */
    private Map<String, String> filters;

    /**
     * 聚合统计类型
     */
    private String aggregationType;

    /**
     * 查询条件构造器
     */
    private QueryBuilder queryBuilder;

}

