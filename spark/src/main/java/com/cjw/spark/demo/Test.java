package com.cjw.spark.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author Ego
 * @version 1.0
 * @date 2020/3/24 21:34
 * @since JDK1.8
 */
public class Test {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("calOccurrences").setMaster("local").set("spark.driver.host", "localhost");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> textFile = sc.textFile("input.txt");
        JavaPairRDD<String, Integer> counts = textFile
                .flatMap(s -> Arrays.asList(s.split(System.lineSeparator())).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);
        counts.foreach(stringIntegerTuple2 -> {
            System.out.println(stringIntegerTuple2.toString());
        });

    }
}
