package com.packtpub.spark.module_four.chapter_11

import org.apache.spark.sql.SparkSession

object ConsumingData {

  def main(args: Array[String]): Unit = {

    // Build a SparkSession in Local Mode
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("My Spark App")
      .getOrCreate()
  }
}
