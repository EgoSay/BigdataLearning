package com.cjw.bigdata.topn

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.immutable.TreeMap

/**
 * @author Ego
 * @since 2019/12/17 21:44
 * @version 1.0
 */
object GlobalTopN {

  val wordCount: DataStream[(String, Int)] = CommonUtil.calWordCount("localhost", 9000)
  wordCount.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(20)))

  private class GlobalTopNFunction extends ProcessAllWindowFunction[(String, Int), (String, Int), TimeWindow] {

    private val topSize = 10

    override def process(context: Context,
                         elements: Iterable[(String, Int)],
                         out: Collector[(String, Int)]): Unit = {
      // TODO 利用 TreeMap构造自定义排序
    }
  }
}
