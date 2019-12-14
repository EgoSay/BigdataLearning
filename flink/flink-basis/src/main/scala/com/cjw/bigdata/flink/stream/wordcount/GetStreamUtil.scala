package com.cjw.bigdata.flink.stream.wordcount

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * @author Ego
 * @since 2019/12/14 11:34
 * @version 1.0
 */
object GetStreamUtil {

  private val ip = "localhost"
  private val port = 9000

  def getSocketTextStream(env: StreamExecutionEnvironment): DataStream[String] = {
    env.socketTextStream(ip, port)
  }

}
