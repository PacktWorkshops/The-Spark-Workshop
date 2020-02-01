package com.packtpub.spark.module_four.chapter_12.streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TransformingStreams {

  def main(args: Array[String]): Unit = {

    // Build a SparkSession in Local Mode
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("My Spark App")
      .getOrCreate()

    val sc = spark.sparkContext

    // Create a StreamingContext
    val ssc = new StreamingContext(sc, Seconds(10))

    // Create a list of servers
    val servers = Seq("localhost:9092").mkString(",")

    val kafka_params = Map[String, Object](
      "bootstrap.servers" -> servers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "streaming_test",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))

    val topics = Array("website_traffic")
    val strategy = ConsumerStrategies.Subscribe[String, String](topics, kafka_params)

    val rawData = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      strategy
    )

    // Create a dictionary/lookup of actions (int) to their description
    val actions = Seq((1, "opened website"), (2, "clicked"), (3, "scrolled"))
    val webActions =  spark.sparkContext.parallelize(actions)

    // Turn the raw stream data into
    val parsedWebTraffic = rawData.map(x => {
      val parts = x.value().split(",")
      val userID = parts{0}
      val actionID = parts{1}.toInt
      (actionID, userID)
    })


    parsedWebTraffic.print()

    // Use transform method to join data with lookup
    val enhanced = parsedWebTraffic.transform(x => {
      x.join(webActions)
    })
    enhanced.print()

    // Allow the streaming context to start, and stick around until terminated
    ssc.start()
    ssc.awaitTermination()
  }
}
