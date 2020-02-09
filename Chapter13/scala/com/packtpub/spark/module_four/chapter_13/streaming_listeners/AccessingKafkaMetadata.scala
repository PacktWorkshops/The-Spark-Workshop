package com.packtpub.spark.module_four.chapter_13.streaming_listeners

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object AccessingKafkaMetadata {

  def main(args: Array[String]): Unit = {

    // Build a SparkSession in Local Mode
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("My Spark App")
      .getOrCreate()

    val sc = spark.sparkContext

    // Create a StreamingContext
    val ssc = new StreamingContext(sc, Seconds(20))

    // Add Custom Listener
    ssc.addStreamingListener(new WorkshopStreamListener)

    // Create a list of servers
    val servers = Seq("localhost:9092")
    val bootstrap_servers = servers.mkString(",")

    val kafka_params = Map[String, Object](
      "bootstrap.servers" -> bootstrap_servers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "streaming_test",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))

    val topics = Array("data")
    val strategy = ConsumerStrategies.Subscribe[String, String](topics, kafka_params)

    val rawData = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      strategy
    )

    // Print numRecords received in the batch
    rawData.count().print()

    // Print first 3 records received in the batch
    val data = rawData.map(_.value())

    data.print()

    // Allow the streaming context to start, and stick around until terminated
    ssc.start()
    ssc.awaitTermination()
  }
}
