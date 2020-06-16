package com.cjw.bigdata.topn

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{SlidingProcessingTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.immutable.TreeMap

/**
 * 分组 TopN, 实现了按照首字母分组，取每组元素 count 最高的 TopN
 *
 * @author Ego
 * @date 2019/12/23 21:19
 * @version 1.0
 * @since JDK1.8
 */
object GroupTopN {

  val hostname = "localhost"
  val port = 9000

  private class GroupTopNFunction extends ProcessWindowFunction[(String, Int), (String, Int), String, TimeWindow] {

    private var topSize = 3

    def this(topSize: Int) {
      this()
      // TODO Auto-generated constructor stub
      this.topSize = topSize
    }

    override def process(key: String,
                         context: Context,
                         elements: Iterable[(String, Int)],
                         out: Collector[(String, Int)]): Unit = {

      val treeMap = new TreeMap[Int, (String, Int)]
      elements.foreach(ele => {
        // 只保留 TopN元素
        if (treeMap.size > topSize) {
          treeMap.drop(treeMap.size - topSize)
        }
      })
      for(value <- treeMap.values) {
        out.collect(value)
      }
    }
  }

  def main(args: Array[String]): Unit = {
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

    wordCount.map(_._1.substring(0, 1))
      .keyBy(0)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
      .process(new GroupTopNFunction(3))


  }
}
