package com.cjw.spark.dataskew;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * 数据倾斜解决方案1
 * @author Ego
 * @version 1.0
 * @date 2020/11/1 17:05
 * @since JDK1.8
 */
public class Solution1 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("calOccurrences").setMaster("local").set("spark.driver.host", "localhost");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> textFile = sc.textFile("input.txt");
        JavaPairRDD<String, Integer> rdd = textFile
                .flatMap(s -> Arrays.asList(s.split(System.lineSeparator())).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);

    }
}
