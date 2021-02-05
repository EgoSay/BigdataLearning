package com.cjw.bigdata.spark.es;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.Metadata;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.spark_project.guava.collect.ImmutableList;
import org.spark_project.guava.collect.ImmutableMap;
import scala.Tuple2;

import java.util.Map;

import static org.elasticsearch.spark.rdd.Metadata.ID;
import static org.elasticsearch.spark.rdd.Metadata.VERSION;

/**
 * Spark读写es
 * refer: https://www.elastic.co/guide/en/elasticsearch/hadoop/current/spark.html
 * @author Ego
 * @version 1.0
 * @date 2021/2/4 17:46
 * @since JDK1.8
 */
@Slf4j
public class SparkEsOperation {

    private static SparkConf getConf() {
        return new SparkConf().setAppName("SparkEsOperation").setMaster("local[1]").clone()
                .set("es.nodes", "127.0.0.1")
                .set("es.port", "9200")
                .set("es.nodes.wan.only", "true")
                .set("es.batch.write.refresh", "true");
    }


    /**
     * 测试写 ES 数据
     */
    public static void writeData() {
        JavaSparkContext sc = new JavaSparkContext(getConf());

        Map<String, ?> numbers = ImmutableMap.of("one", 1, "two", 2);
        Map<String, ?> airports = ImmutableMap.of("t1", "hello", "t2", "San Fran");
        JavaRDD<Map<String, ?>> javaRDD = sc.parallelize(ImmutableList.of(numbers, airports));
        JavaEsSpark.saveToEs(javaRDD, "spark/_doc");
        log.info(">>>>>>>>>>>>>>>>> write map data to Es successfully....");

        // Writing existing JSON to es
        String json1 = "{\"id\": 1, \"name\": \"zhangsan\", \"birth\": \"1990-01-01\", \"addr\": \"No.969, wenyixi Rd, yuhang, hangzhou\"}";
        String json2 = "{\"id\": 2, \"name\": \"lisi\", \"birth\": \"1991-01-01\", \"addr\": \"No.556, xixi Rd, xihu, hangzhou\"}";
        String json3 = "{\"id\": 3, \"name\": \"wangwu\", \"birth\": \"1992-01-01\", \"addr\": \"No.699 wangshang Rd, binjiang, hangzhou\"}";
        JavaRDD<String> jsonRdd = sc.parallelize(ImmutableList.of(json1, json2, json3));
        JavaEsSpark.saveJsonToEs(jsonRdd, "json/_doc");
        log.info(">>>>>>>>>>>>>>>>> write json data to Es successfully....");

        // data to be saved
        Map<String, ?> otp = ImmutableMap.of("iata", "OTP", "name", "Otopeni");
        Map<String, ?> sfo = ImmutableMap.of("iata", "SFO", "name", "San Fran");

        // write metadata for each document
        // note it's not required for them to have the same structure
        Map<Metadata, Object> otpMeta = ImmutableMap.of(ID, 1);
        Map<Metadata, Object> sfoMeta = ImmutableMap.of(ID, "2", VERSION, "2");
        // create a pair RDD between the id and the docs
        JavaPairRDD<?, ?> pairRdd = sc.parallelizePairs((ImmutableList.of(
                new Tuple2<>(otpMeta, otp),
                new Tuple2<Object, Object>(sfoMeta, sfo))));
        JavaEsSpark.saveToEsWithMeta(pairRdd, "meta/_doc");
    }

    public static void readData() {
        JavaSparkContext sc = new JavaSparkContext(getConf());

        // read data
        JavaPairRDD<String, Map<String, Object>> jsonRdd = JavaEsSpark.esRDD(sc, "json/_doc");
        log.info(">>>>>>>>>>>>> read es data from json/_doc:{}", JSONObject.toJSONString(jsonRdd.values().collect()));

        // with lambda operation
        JavaRDD<Map<String, Object>> filter = jsonRdd.values().filter(doc -> doc.containsValue("zhangsan"));
        log.info(">>>>>>>>>>>>> read es data from json/_doc with lambda operation :{}", JSONObject.toJSONString(filter.count()));

    }

    public static void main(String[] args) {
        // writeData();

        readData();
    }



}
