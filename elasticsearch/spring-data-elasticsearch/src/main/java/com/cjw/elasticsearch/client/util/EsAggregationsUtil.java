package com.cjw.elasticsearch.client.util;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.ObjectUtil;
import com.cjw.elasticsearch.client.domain.EsAggregationsContext;
import com.cjw.elasticsearch.common.RestClientHelper;
import com.cjw.elasticsearch.common.constant.EsOperationConstant;
import com.cjw.elasticsearch.common.exception.EsOperationException;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.stats.StatsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.joda.time.DateTimeZone;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Es
 * @author Ego
 * @version 1.0
 * @date 2020/9/23 14:17
 * @since JDK1.8
 */
@Slf4j
@Component
public class EsAggregationsUtil {

    @Resource
    private RestClientHelper restClientHelper;
    private RestHighLevelClient client;

    @PostConstruct
    private void init() {
        client = restClientHelper.getClient();
    }


    /**
     * 统计字段(最大/最小/平均值....)
     * @param aggregationsContext
     * @return
     */
    public Stats getFieldStatsAggregations(EsAggregationsContext aggregationsContext) {

        if (ObjectUtil.isNull(aggregationsContext.getAggregationType())) {
            aggregationsContext.setAggregationType(EsOperationConstant.AggregationType.STATS.getCode());
        }

        SearchRequest searchRequest = new SearchRequest();
        StatsAggregationBuilder agg = AggregationBuilders.stats(aggregationsContext.getAggregationType()).field(aggregationsContext.getField());
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().aggregation(agg);

        // 如果有组合查询条件, 添加查询请求
        if (ObjectUtil.isNotNull(aggregationsContext.getQueryBuilder())) {
            sourceBuilder.query(aggregationsContext.getQueryBuilder());
        }

        searchRequest.indices(aggregationsContext.getIndices()).source(sourceBuilder);
        log.info(">>>>>>>>>>Es查询请求: {}", sourceBuilder.toString());
        try {
            SearchResponse searchResponse = client.search(searchRequest);
            Stats stats = searchResponse.getAggregations().get(aggregationsContext.getAggregationType());
            return stats;
        } catch (Exception e) {
            throw new EsOperationException(e);
        }
    }

    /**
     * 针对数值型标签数据分布的直方图统计
     * @param aggregationsContext
     * @return
     */
    public Map<String,Object> getNumericHistogram(EsAggregationsContext aggregationsContext) {

        // 构造 HistogramAggregationBuilder
        if (ObjectUtil.isNull(aggregationsContext.getInterval())
                || NumberUtil.isLessOrEqual(BigDecimal.valueOf(aggregationsContext.getInterval()), BigDecimal.ZERO)) {

            // 默认设置为10
            aggregationsContext.setInterval(10.0);
        }

        if (ObjectUtil.isNull(aggregationsContext.getAggregationType())) {
            aggregationsContext.setAggregationType(EsOperationConstant.AggregationType.NUMERIC.getCode());
        }

        HistogramAggregationBuilder histogramAggregationBuilder = AggregationBuilders
                .histogram(aggregationsContext.getAggregationType())
                .field(aggregationsContext.getField())
                .interval(aggregationsContext.getInterval())
                .minDocCount(0)
                .offset(aggregationsContext.getOffset())
                .order(BucketOrder.key(false));

        return getAggregationDataMap(aggregationsContext, histogramAggregationBuilder);
    }

    /**
     * 时间类型数据分布的直方图统计
     * @param aggregationsContext
     * @return
     */
    public Map<String,Object> getDateHistogram(EsAggregationsContext aggregationsContext) {

        // 时间类型
        if (ObjectUtil.isNull(aggregationsContext.getDateHistogramInterval())) {
            // 默认为按 月度进行统计
            aggregationsContext.setDateHistogramInterval(DateHistogramInterval.MONTH);
        }
        if (ObjectUtil.isNull(aggregationsContext.getAggregationType())) {
            aggregationsContext.setAggregationType(EsOperationConstant.AggregationType.DATE.getCode());
        }
        if (ObjectUtil.isNull(aggregationsContext.getDateFormat())) {
            aggregationsContext.setDateFormat(DatePattern.NORM_DATE_PATTERN);
        }

        DateHistogramAggregationBuilder dateHistogramAggregationBuilder = AggregationBuilders
                .dateHistogram(aggregationsContext.getAggregationType())
                .field(aggregationsContext.getField())
                .dateHistogramInterval(aggregationsContext.getDateHistogramInterval())
                .timeZone(DateTimeZone.forOffsetHours(8))
                .format(aggregationsContext.getDateFormat())
                .order(BucketOrder.key(true));


        return getAggregationDataMap(aggregationsContext, dateHistogramAggregationBuilder);
    }

    /**
     * 根据过滤条件进行分组统计
     * @param aggregationsContext
     * @return
     */
    public Map<String, Object> getFiltersAggregations(EsAggregationsContext aggregationsContext) {

        if (ObjectUtil.isNull(aggregationsContext.getAggregationType())) {
            aggregationsContext.setAggregationType(EsOperationConstant.AggregationType.FILTERS.getCode());
        }

        Map<String, String> filters = aggregationsContext.getFilters();
        List<FiltersAggregator.KeyedFilter> keyedFilters = new ArrayList<>();
        if (!CollectionUtils.isEmpty(filters)) {
            filters.forEach((k, v) -> {
                FiltersAggregator.KeyedFilter keyedFilter = new FiltersAggregator.KeyedFilter(k, QueryBuilders.termsQuery(k, v));
                keyedFilters.add(keyedFilter);
            });
        }
        AggregationBuilder filtersAggregationBuilder = new FiltersAggregationBuilder(
                aggregationsContext.getAggregationType(),
                keyedFilters.toArray(new FiltersAggregator.KeyedFilter[0]));


        return getAggregationDataMap(aggregationsContext, filtersAggregationBuilder);
    }


    private Map<String, Object> getAggregationDataMap(EsAggregationsContext context, AggregationBuilder aggregationBuilder) {
        Map<String,Object> aggregationDataMap = new HashMap<>();
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().aggregation(aggregationBuilder);

        // 如果有组合查询条件, 添加查询请求
        if (ObjectUtil.isNotNull(context.getQueryBuilder())) {
            // 只筛选不返回查询数据
            sourceBuilder.query(context.getQueryBuilder()).size(0);
        }

        searchRequest.indices(context.getIndices()).source(sourceBuilder);
        log.info(">>>>>>>>>>Es 图表数据查询请求: {}", sourceBuilder.toString());
        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            MultiBucketsAggregation multiBucketsAggregation = searchResponse.getAggregations().get(context.getAggregationType());
            for (MultiBucketsAggregation.Bucket entry : multiBucketsAggregation.getBuckets()) {
                // 区间key
                String key = entry.getKeyAsString();
                // Doc count
                long docCount = entry.getDocCount();
                //数据封装
                aggregationDataMap.put(key, docCount);

            }

        } catch (Exception e) {
            throw new EsOperationException(e);
        }
        return aggregationDataMap;
    }
}
