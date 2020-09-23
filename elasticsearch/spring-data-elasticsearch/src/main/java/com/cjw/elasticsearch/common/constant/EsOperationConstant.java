package com.cjw.elasticsearch.common.constant;

/**
 * Es 操作常量类
 * @author Ego
 * @version 1.0
 * @date 2020/9/23 14:25
 * @since JDK1.8
 */
public class EsOperationConstant {


    public static final long SCROLL_KEEPALIVE_SECONDS = 30L;
    public static final int FETCH_SIZE = 1000;

    /**
     *
     */
    public enum QueryType {
        /**
         * K, V类型查询
         */
        DSL("dsl"),

        /**
         * term查询
         */
        TERMS( "terms"),

        /**
         * nested查询
         */
        NESTED("nested"),

        /**
         * Range 范围查询
         */
        RANGE( "range");

        private String code;

        public String getCode() {
            return code;
        }


        QueryType(String code) {
            this.code = code;

        }
    }

    public enum AggregationType {

        /**
         * 基本统计
         */
        STATS("stats"),

        /**
         * 数值型
         */
        NUMERIC("numeric"),

        /**
         * 时间型
         */
        DATE("date"),

        /**
         * 过滤条件分组型
         */
        FILTERS("filters");


        private String code;

        AggregationType(String code) {
            this.code = code;
        }

        public String getCode() {
            return code;
        }
    }

}