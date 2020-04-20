package com.packtpub.spark.module_four.chapter_12.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Exercise12_02 {

  def main(args: Array[String]): Unit = {

    // Build a SparkSession in Local Mode
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("My Spark App")
      .getOrCreate()

    val sc = spark.sparkContext

    // Create a StreamingContext
    val ssc = new StreamingContext(sc, Seconds(1))

    // Make a DStream of type String, `lines`, that will store every entry received from Netcat
    val lines = ssc.socketTextStream("localhost", 9999)

    // Every line is a sentence, so split on space to create a Dataset of words
    val words = lines.flatMap(_.split(" "))

    // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()

    // Allow the streaming context to start, and stick around until terminated
    ssc.start()
    ssc.awaitTermination()
  }
}
