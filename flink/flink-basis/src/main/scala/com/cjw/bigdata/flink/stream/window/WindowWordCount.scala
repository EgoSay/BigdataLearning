package com.cjw.bigdata.flink.stream.window

import com.cjw.bigdata.flink.stream.wordcount.GetStreamUtil
import org.apache.flink.streaming.api.scala._

/**
 * Count Window Example
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


    val windowSize = 5
    val slideSize = 3

    val windowWordCount = text
      .flatMap(_.toLowerCase().split(","))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      // countWindow 计数窗口，采用事件数量作为窗口处理依据，这里是滑动窗口，每 3 个事件计算最近 5 个事件消息
      .countWindow(windowSize, slideSize)
      .sum(1) // 将第2个元素即 count 值累加

    windowWordCount.print().setParallelism(1)

    env.execute("Socket Window WordCount")
  }
}
