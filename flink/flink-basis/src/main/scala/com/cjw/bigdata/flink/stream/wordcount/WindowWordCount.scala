package com.cjw.bigdata.flink.stream.wordcount

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * Implements a windowed version of the streaming "SocketWordCount" program
 *
 * @author Ego
 * @since 2019/12/14 11:30
 * @version 1.0
 */

object WindowWordCount {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = GetStreamUtil.getSocketTextStream(env)


    val windowSize = 10
    val slideSize = 5

    val windowWordCount = text
      .flatMap(_.toLowerCase().split(","))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .countWindow(windowSize, slideSize)
      .sum(1) // 将第2个元素即 count 值累加

    windowWordCount.print().setParallelism(1)

    env.execute("Socket Window WordCount")
  }
}
