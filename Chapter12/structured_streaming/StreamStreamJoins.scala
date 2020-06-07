package com.packtpub.spark.module_four.chapter_12.structured_streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._

object StreamStreamJoins {

  case class RateSource(timestamp: Timestamp, value: Long)

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
    val streamOne = spark.readStream
      .format("rate")
      .option("rowsPerSecond", "10")
      .load()
      .as[RateSource]

    val streamOneWithWatermark = streamOne.withWatermark("timestamp", "1 minute")
      .withColumnRenamed("timestamp", "timestampOne")
      .withColumnRenamed("value" , "valueOne")

    val streamTwo = spark.readStream
      .format("rate")
      .option("rowsPerSecond", "10")
      .load()
      .as[RateSource]

    val streamTwoWithWatermark = streamTwo.withWatermark("timestamp", "1 minute")
      .withColumnRenamed("timestamp", "timestampTwo")
      .withColumnRenamed("value" , "valueTwo")
    
    val joinedStream = streamOneWithWatermark.join(streamTwoWithWatermark, expr("""
    valueOne = valueTwo AND
    timestampTwo >= timestampOne AND
    timeStampTwo <= timestampOne + interval 1 hour
    """))

    // Generate running word count
    val rowCounts = joinedStream.groupBy("valueOne", "timestampOne").count()

    // Start running the query that prints the running counts to the console
    val query = rowCounts.writeStream
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .format("console")
      .start()

    query.awaitTermination()
  }
}
