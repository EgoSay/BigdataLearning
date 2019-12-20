package com.cjw.bigdata.topn

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{SlidingProcessingTimeWindows, TumblingProcessingTimeWindows}
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

  val hostname = "localhost"
  val port = 9000

  private class GlobalTopNFunction extends ProcessAllWindowFunction[(String, Int), (String, Int), TimeWindow] {

    private var topSize = 3

    def this(topSize: Int) {
      this()
      // TODO Auto-generated constructor stub
      this.topSize = topSize
    }

    override def process(context: Context,
                         elements: Iterable[(String, Int)],
                         out: Collector[(String, Int)]): Unit = {
      // TODO 利用 TreeMap构造自定义排序 ? Ordering 用法需要再熟悉一下
      //val treeMap = TreeMap[Int, (String, Int)]()(Ordering.by()
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

    //windowAll 是一个全局并发为 1 的特殊操作，也就是所有元素都会进入到一个窗口内进行计算
    wordCount.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(20))).
      process(new GlobalTopNFunction(3))

    wordCount.print().setParallelism(1)
    env.execute()
  }


}
