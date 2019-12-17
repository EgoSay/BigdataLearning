package com.cjw.bigdata.topn

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author Ego
 * @date 2019/12/13 22:25
 * @version 1.0
 */
object CommonUtil {

  val hostname = "localhost"
  val port = 9000

  def calWordCount(hostname: String, port: Int): DataStream[(String, Int)] = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 因为做实时统计 TopN， 所以这里利用 Processing Time
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val text = env.socketTextStream(hostname, port)

    val wordCount: DataStream[(String, Int)] = text
      .flatMap(_.toLowerCase().split(","))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      // key 之后的元素进入一个总时间长度为600s, 每20s向后滑动一次的滑动窗口
      .window(SlidingProcessingTimeWindows.of(Time.seconds(600), Time.seconds(20)))
      .sum(1)

    wordCount
  }
}
