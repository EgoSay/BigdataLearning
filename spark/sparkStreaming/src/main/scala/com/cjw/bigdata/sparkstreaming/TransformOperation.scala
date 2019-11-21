package com.cjw.bigdata.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 利用 transform 算子进行黑名单过滤操作
 * @author Ego
 * @since 2019/11/21 12:21
 * @version 1.0
 */
object TransformOperation {

  def main(args: Array[String]): Unit = {
    // 初始化一个StreamingContext
    val conf = new SparkConf().setMaster("local[2]").setAppName("TransformOperation")
    val ssc = new StreamingContext(conf, Seconds(5))
    val lines = ssc.socketTextStream("localhost", 9999)

    /**
     * 初始化一个黑名单列表，实际中黑名单列表应该从数据库中拿
     * 黑名单列表转换成 [(black1, true), (black2, true)]格式， 加一个 true 标志其是黑名单
     */
    val blackList = List("black1", "black2")
    val blackListRDD = ssc.sparkContext.parallelize(blackList).map(x => (x, true))

    // 数据格式是 (20191121, black1) 转换成 (black1: (20191121, black1)), 主要是为了提取出关键字做 key
    val keyBlackList = lines.map(x => (x.split(",")(1), x))

    // 黑名单过滤操作，先和黑名单列表RDD join， 然后判断标志是否为 true， 是的话代表是黑名单进行过滤，最后将数据还原成初始格式打印
    val filterCount = keyBlackList.transform(rdd => {
      rdd.leftOuterJoin(blackListRDD)
        .filter(x => !x._2._2.getOrElse(false))
        .map(x => x._2._1)
    })

    filterCount.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
