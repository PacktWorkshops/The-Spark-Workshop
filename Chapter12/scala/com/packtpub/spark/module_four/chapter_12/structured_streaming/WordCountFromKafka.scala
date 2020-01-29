package com.packtpub.spark.module_four.chapter_12.structured_streaming

import org.apache.spark.sql.SparkSession

object WordCountFromKafka {

  def main(args: Array[String]): Unit = {

    // Build a SparkSession in Local Mode
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("My Spark App")
      .getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    // Make a DataFrame, `lines`, that will store every entry received from Netcat
    val lines = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .load()

    // fetch key and value from Kafka
    val words = lines.selectExpr("CAST(key as STRING) as key", "CAST(value as STRING) as word")
      .as[(String, String)]

    // Generate running word count
    val wordCounts = words.groupBy("word").count()

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
