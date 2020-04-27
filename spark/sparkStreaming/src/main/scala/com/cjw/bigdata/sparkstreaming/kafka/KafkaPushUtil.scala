package com.cjw.bigdata.sparkstreaming.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 向Kafka写数据
 *
 * @author Ego
 * @date 2020/4/24 15:02
 * @version 1.0
 * @since JDK1.8
 */
object KafkaPushUtil extends Logging{

  def main(args: Array[String]): Unit = {
    // 初始化一个StreamingContext
    val conf = new SparkConf().setMaster("local[2]").setAppName("KafkaPushUtil")
    val ssc = new StreamingContext(conf, Seconds(5))

    val lines = ssc.socketTextStream("localhost", 9999)

    // 广播KafkaSink
    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", "10.111.32.81:9092")
        p.setProperty("key.serializer", classOf[StringSerializer].getName)
        p.setProperty("value.serializer", classOf[StringSerializer].getName)
        p
      }
      log.warn("kafka producer init done!")
      ssc.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }

    // 两种方式向Kafka写入数据
    directSend(lines)
    broadcastSend(lines, kafkaProducer)

    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * 直接输出
   * @param input
   */
  def directSend(input: ReceiverInputDStream[String]): Unit ={
    log.info("************直接输出向Kafka写数据*****************")

    input.foreachRDD(rdd => {

      // 不能在这里新建KafkaProducer，因为KafkaProducer是不可序列化的
      rdd.foreachPartition(partition => {
        partition.foreach{
          case x: String=>{
            println(x)
            val props = new Properties()
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop1:6667")
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
              "org.apache.kafka.common.serialization.StringSerializer")
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
              "org.apache.kafka.common.serialization.StringSerializer")
            val producer = new KafkaProducer[String, String](props)
            val message=new ProducerRecord[String, String]("KafkaPushTest1","KafkaPushTest1",x)
            producer.send(message)

          }

        }
      })
    })
  }

  /**
   * 利用广播机制
   * @param input
   */
  def broadcastSend(input: ReceiverInputDStream[String], kafkaProducer: Broadcast[KafkaSink[String, String]]): Unit ={
    log.info("************利用广播机制向Kafka写数据*****************")
    input.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        rdd.foreach(record => {
          kafkaProducer.value.send("KafkaPushTest2","KafkaPushTest2", record)
        })
      }
    })
  }

}
