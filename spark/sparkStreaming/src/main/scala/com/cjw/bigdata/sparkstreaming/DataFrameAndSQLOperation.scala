package com.cjw.bigdata.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Spark Streaming整合Spark SQL完成词频统计操作
 *
 * @author Ego
 * @since 2019/11/22 16:50
 * @version 1.0
 */
object DataFrameAndSQLOperation {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("DataFrameAndSQLOperation")
    val ssc = new StreamingContext(conf, Seconds(5))

    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(","))

    words.foreachRDD(rdd => {
      val spark = SingletonSparkSession.getInstance(rdd.sparkContext.getConf)
      import spark.implicits._

      val wordsDataFrame = rdd.toDF("word")
      wordsDataFrame.createOrReplaceTempView("words")

      val wordCountDataFrame = spark.sql("select word, count(*) as total from words group by word")
      wordCountDataFrame.show()
    })

    ssc.start()
    ssc.awaitTermination()
  }
}

//case class Record(word: String)

object SingletonSparkSession {

  @transient private var instance: SparkSession = _

  def getInstance(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession.builder().config(sparkConf).getOrCreate()
    }
    instance
  }
}