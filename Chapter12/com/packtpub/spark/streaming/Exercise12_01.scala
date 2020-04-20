package com.packtpub.spark.module_four.chapter_12.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Exercise12_01 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
    .builder()
    .master("local[2]")
    .appName("My Spark App")
    .getOrCreate()

    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(5))
    
  }
}
