package com.cjw.bigdata.sparkstreaming.kafka

import java.util.concurrent.Future

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}


/**
 * 向kafka中写入数据 改进
 *
 * @author Ego
 * @date 2020/4/26 23:15
 * @version 1.0
 * @since JDK1.8
 */
class KafkaSink[K, V](createProducer: () => KafkaProducer[K, V]) extends Serializable {

  lazy val producer = createProducer()

  def send(topic: String, key: K, value: V): Future[RecordMetadata] =
    producer.send(new ProducerRecord[K, V](topic, key, value))
  def send(topic: String, value: V): Future[RecordMetadata] =
    producer.send(new ProducerRecord[K, V](topic, value))

}

object KafkaSink {
  import scala.collection.JavaConversions._

  def apply[K,V](config: Map[String,Object]):KafkaSink[K,V]= {
    val createProducerFunc = () => {
      val producer = new KafkaProducer[K,V](config)
      sys.addShutdownHook{
        producer.close()
      }
      producer
    }
    new KafkaSink(createProducerFunc)
  }

  def apply[K, V](config: java.util.Properties): KafkaSink[K, V] = apply(config.toMap)

}
