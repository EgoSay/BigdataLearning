package com.cjw.bigdata.flink.stream.window

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author Ego
 * @since 2019/12/15 20:13
 * @version 1.0
 */
object TimeWindowing {
  val ip = "localhost"
  val port = 9000

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream(ip, port)

    val wordCount = text
      .flatMap(_.toLowerCase().split(","))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      // TimeWindow 有滚动和滑动窗口两种，这里是滚动窗口
      .timeWindow(Time.seconds(5))
      .sum(1)

    wordCount.print().setParallelism(1)

    env.execute("Socket Window WordCount")
  }
}
