package com.cjw.bigdata.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 使用 updateStateByKey 算子实时统计求和
 * @author Ego
 * @since 2019/11/20 21:11
 * @version 1.0
 */
object StatefulWordCount {

  /**
   * 更新状态函数,  这里就是一个简单的统计求和
   * @param currentValues  当前值
   * @param preValues  前面保存的状态值
   * @return
   */
  def updateFunction(currentValues: Seq[Int], preValues: Option[Int]): Option[Int] = {

    val current = currentValues.sum
    val pre = preValues.getOrElse(0)

    Some(current + pre)
  }

  def main(args: Array[String]): Unit = {

    // 初始化一个StreamingContext
    val conf = new SparkConf().setMaster("local[2]").setAppName("StatefulWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))

    // 必须设置 checkpoint ， 建议设置为 HDFS 上某个指定位置
    ssc.checkpoint("hdfs://hadoop2:8020/user/spark/checkpoint")

    val lines = ssc.socketTextStream("localhost", 9999)
    val state = lines.flatMap(_.split(",")).map((_, 1))
    val stateWordCount = state.updateStateByKey(updateFunction)

    stateWordCount.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
