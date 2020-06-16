package com.cjw.bigdata.sparkstreaming

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 计算TopN
 * @author Ego
 * @date 2020/3/1 16:47
 * @version 1.0
 * @since JDK1.8
 */
object TopN {
  def main(args: Array[String]): Unit = {
    // 初始化一个StreamingContext
    val conf = new SparkConf().setMaster("local[2]").setAppName("StatefulWordCount")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("/Users/chenjiawei/Desktop/text.txt")


    lines.flatMap(_.toLowerCase.split(" "))
      .filter(_.nonEmpty)
      .map(x => (x.toInt, null))
      .sortByKey(false)
      .take(5)
      .map(_._1)
      .foreach(println)
  }

}
