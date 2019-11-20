package com.cjw.bigdata.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Spark Streaming 简单示例，读取 TCP Socket 流数据
 * @author Ego
 * @since 2019/11/20 20:37
 * @version 1.0
 */
object NetworkWordCount {

  def main(args: Array[String]): Unit = {

    // 初始化一个StreamingContext
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))

    val lines = ssc.socketTextStream("localhost", 9999)
    val wordCount = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    wordCount.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
