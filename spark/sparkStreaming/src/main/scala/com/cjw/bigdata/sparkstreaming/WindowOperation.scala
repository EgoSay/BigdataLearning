package com.cjw.bigdata.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Spark Streaming 窗口操作
 * @author Ego
 * @since 2019/11/21 18:20
 * @version 1.0
 */
object WindowOperation {

  def main(args: Array[String]): Unit = {
    // 初始化一个StreamingContext
    val conf = new SparkConf().setMaster("local[2]").setAppName("WindowOperation")
    val ssc = new StreamingContext(conf, Seconds(5))
    val lines = ssc.socketTextStream("localhost", 9999)

    // 统计某个时间窗口的 wordCount
    val windowWordCount = lines.flatMap(_.split(",")).map((_, 1)).reduceByKeyAndWindow((a:Int, b:Int) => a + b, Seconds(30), Seconds(20))

    windowWordCount.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
