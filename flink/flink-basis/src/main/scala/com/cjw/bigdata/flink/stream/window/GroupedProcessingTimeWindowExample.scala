package com.cjw.bigdata.flink.stream.window

import java.util.concurrent.TimeUnit.MILLISECONDS
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author Ego
 * @since 2019/12/15 20:26
 * @version 1.0
 */
object GroupedProcessingTimeWindowExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)

    val stream: DataStream[(Long, Long)] = env.addSource(new CustomDataSource)
    stream.keyBy(0)
          .timeWindow(Time.of(2500, MILLISECONDS), Time.of(500, MILLISECONDS))
          .reduce((value1, value2) => (value1._1, value1._2 + value2._2))
          .addSink(new SinkFunction[(Long, Long)]() {
            override def invoke(in: (Long, Long)): Unit = {}
      })

    env.execute()
  }
}

private class CustomDataSource extends RichParallelSourceFunction[(Long, Long)] {
  @volatile private var isRunning = true
  override def run(ctx: SourceFunction.SourceContext[(Long, Long)]): Unit = {
    val startTime = System.currentTimeMillis()
    val numElements = 20000000
    val numKeys = 10000
    var value = 1L
    var count = 0L

    while (isRunning && count < numElements) {

      ctx.collect((value, 1L))

      count += 1
      value += 1

      if (value > numKeys) {
        value = 1L
      }

      val endTime = System.currentTimeMillis()
      println(s"Took ${endTime - startTime} msecs for $numElements values")
    }
  }

  override def cancel(): Unit = isRunning = false
}