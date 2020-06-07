package com.packtpub.spark.module_four.chapter_12.streaming

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Example12_01 {

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
    val servers = Seq("localhost:9092")
    val bootstrap_servers = servers.mkString(",")

    val kafka_params = Map[String, Object](
      "bootstrap.servers" -> bootstrap_servers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "streaming_test",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))

    // dataset design
    // topic one: user browsing events on a desktop website
    // topic two: user browsing events on a mobile app / website

    // assign topics
    val desktopTopic = Array("desktop")
    val mobileTopic = Array("mobile")

    // set strategies
    val desktopStrategy = ConsumerStrategies.Subscribe[String, String](desktopTopic, kafka_params)
    val mobileStrategy = ConsumerStrategies.Subscribe[String, String](mobileTopic, kafka_params)

    // pull topic 1
    val desktopEvents = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      desktopStrategy
    )

    // pull topic 2
    val mobileEvents = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      mobileStrategy
    )

    // turn InputDStreams into DStreams
    val desktopDStream = desktopEvents.filter(_.value().isInstanceOf[String]).map(_.value().asInstanceOf[String])
    val mobileDStream = mobileEvents.filter(_.value().isInstanceOf[String]).map(_.value().asInstanceOf[String])

    case class DesktopEvent(user_id: String, action: String)
    case class MobileEvent(user_id: String, action: String)

    val desktop = desktopDStream.map(x => {
      val parts = x.split(",")
      (parts{0}, DesktopEvent(parts{0}, parts{1}))
    })

    val groupedDesktopEvents = desktop.groupByKey()
    groupedDesktopEvents.print()

   val mobile = mobileDStream.map(x => {
     val parts = x.split(",")
     (parts{0}, MobileEvent(parts{0}, parts{1}))
   })

   val combinedEvents = desktop.join(mobile)

   // turn DStream into a PairedDStream
   combinedEvents.print()

    // Allow the streaming context to start, and stick around until terminated
    ssc.start()
    ssc.awaitTermination()
  }
}
