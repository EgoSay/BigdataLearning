package com.cjw.bigdata.flink.stream.wordcount

import org.apache.flink.streaming.api.scala._

/**
 * @author Ego
 * @since 2019/12/12 18:40
 * @version 1.0
 */
object SocketWordCount {

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
      .sum(1) // 将第2个元素即 count 值累加

    wordCount.print().setParallelism(1)

    env.execute("Socket Window WordCount")
  }

}
