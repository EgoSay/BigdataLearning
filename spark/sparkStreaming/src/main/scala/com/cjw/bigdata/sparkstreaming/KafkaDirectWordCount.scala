package com.cjw.bigdata.sparkstreaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe


/**
 * Spark Streaming 对接 Kafka
 *
 * @author Ego
 * @since 2019/11/26 20:40
 * @version 1.0
 */
object KafkaDirectWordCount {

  def main(args: Array[String]): Unit = {

    // 初始化一个StreamingContext
    val conf = new SparkConf().setMaster("local[2]").setAppName("KafkaDirectWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "hadoop1:6667, hadoop2:6667",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array("test3")
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))

    messages.map(_.value()).map((_, 1)).reduceByKey(_ + _).print()

    ssc.start()
    ssc.awaitTermination()
  }


}
