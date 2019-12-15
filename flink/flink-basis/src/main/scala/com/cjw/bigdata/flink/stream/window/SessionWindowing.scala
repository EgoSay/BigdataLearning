package com.cjw.bigdata.flink.stream.window

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * Session window Example
 *
 * @author Ego
 * @since 2019/12/14 21:51
 * @version 1.0
 */
object SessionWindowing {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置为 EventTime，否则附加时间戳的没有作用了
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val input = List(
      ("a", 1L, 1),
      ("b", 1L, 1),
      ("b", 3L, 1),
      ("b", 5L, 1),
      ("c", 6L, 1),
      ("a", 10L, 1),
      ("c", 11L, 1)
    )

    val source = env.addSource(new SourceFunction[(String, Long, Int)] {
      override def run(sourceContext: SourceFunction.SourceContext[(String, Long, Int)]): Unit = {
        input.foreach(value => {
          // 给数据打上时间戳
          sourceContext.collectWithTimestamp(value, value._2)
          // 设置Watermark, 表示接下来不会再有时间戳小于等于这个数值记录
          sourceContext.emitWatermark(new Watermark(value._2 - 1))
        })
        sourceContext.emitWatermark(new Watermark(Long.MaxValue))
      }

      override def cancel(): Unit = {}
    })

    val aggregated = source.keyBy(0)
      .window(EventTimeSessionWindows.withGap(Time.seconds(3L)))
      .sum(2)

    aggregated.print()

    env.execute()
  }
}
