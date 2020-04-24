package com.cjw.bigdata.sparkstreaming.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
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
object KafkaDirectSend {

  def main(args: Array[String]): Unit = {
    // 初始化一个StreamingContext
    val conf = new SparkConf().setMaster("local[2]").setAppName("KafkaPushUtil")
    val ssc = new StreamingContext(conf, Seconds(5))

    val lines = ssc.socketTextStream("localhost", 9999)

    directSend(lines)
    ssc.start()
    ssc.awaitTermination()
  }

  def directSend(input: ReceiverInputDStream[String]): Unit ={
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
            val message=new ProducerRecord[String, String]("KafkaPushTest1",null,x)
            producer.send(message)

          }

        }
      })
    })
  }

}
