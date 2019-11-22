package com.cjw.bigdata.sparkstreaming

import java.sql.DriverManager

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * foreachRDD 算子完成将结果写入数据库
 * @author Ego
 * @since 2019/11/21 18:52
 * @version 1.0
 */
object ForeachRDDOperation {

  def main(args: Array[String]): Unit = {

    // 初始化一个StreamingContext
    val conf = new SparkConf().setMaster("local[2]").setAppName("ForeachRDDOperation")
    val ssc = new StreamingContext(conf, Seconds(5))

    val lines = ssc.socketTextStream("localhost", 9999)
    val result = lines.flatMap(_.split(",")).map((_, 1)).reduceByKey(_ + _)

    result.print()

    result.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {

        // TODO 利用线程池优化
        val connection = createConnection()
        partitionOfRecords.foreach(record => {

          // 这里无法保证数据正确写入
          val sql = "insert into words(word, count) values('" + record._1 + "'," + record._2 + ")"
          connection.createStatement().execute(sql)
        })
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

  // TODO 可以优化自定义实现一个数据库连接池工具类(HikariCP)
  def createConnection() = {

    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://hadoop1:3306/spark", "spark", "Geotmt_123")
  }
}

