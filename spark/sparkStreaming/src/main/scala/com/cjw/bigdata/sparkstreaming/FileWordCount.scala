package com.cjw.bigdata.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Spark Streaming 简单示例, 读取指定目录下的数据源
 *
 * @author Ego
 * @since 2019/11/20 20:53
 * @version 1.0
 */
object FileWordCount {

  def main(args: Array[String]): Unit = {

    // 初始化一个StreamingContext
    val conf = new SparkConf().setMaster("local[2]").setAppName("FileWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))

    val lines = ssc.textFileStream("hdfs://hadoop2:8020/user/spark/")
    val wordCount = lines.flatMap(_.split(",")).map((_, 1)).reduceByKey(_ + _)
    wordCount.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
