package com.cjw.bigdata.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Spark Streaming 整合 Flume
 * Flume 从指定端口收集数据 sink 到 指定的某个网络端口，然后 Spark Streaming处理这个端口的数据流
 * Flume 配置文件 见 flume-push-streaming.conf
 *
 * @author Ego
 * @since 2019/11/27 00:23
 * @version 1.0
 */

object FlumePushUtil {

  /**
   *
   * @param args args[0]: hostname args[1]: port
   */
  def main(args: Array[String]): Unit = {
    if(args.length != 2) {
      System.err.println("Usage: FlumePushWordCount <hostname> <port>")
      System.exit(1)
    }

    val Array(hostname, port) = args

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("FlumePush")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val flumeStream = FlumeUtils.createStream(ssc, hostname, port.toInt)

    flumeStream.map(x=> new String(x.event.getBody.array()).trim)
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
