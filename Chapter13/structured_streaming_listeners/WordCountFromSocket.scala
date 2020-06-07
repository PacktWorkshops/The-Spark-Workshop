package com.packtpub.spark.module_four.chapter_13.structured_streaming_listeners

import org.apache.spark.sql.SparkSession

object WordCountFromSocket {

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

    // add custom listener
    spark.streams.addListener(new WorkshopStructuredStreamListener)

    // Make a DataFrame, `lines`, that will store every entry received from Netcat
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    // Every line is just a string received from the stream, so turn it into Dataset
    val linesDataset = lines.as[String]

    // Every line is a sentence, so split on space to create a Dataset of words
    val words = linesDataset.flatMap(_.split(" "))
      .withColumnRenamed("value", "word")

    // Generate running word count
    val wordCounts = words.groupBy("word").count()

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination(timeoutMs = 10000)
    query.stop()

  }
}
