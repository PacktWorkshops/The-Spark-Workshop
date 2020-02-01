package com.packtpub.spark.module_four.chapter_12.streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WriteStreamtoHDFS {

  def main(args: Array[String]): Unit = {

    // Build a SparkSession in Local Mode
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("My Spark App")
      .enableHiveSupport()
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

    val topics = Array("test")
    val strategy = ConsumerStrategies.Subscribe[String, String](topics, kafka_params)

    val rawData = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      strategy
    )

    // Print first 3 records received in the batch
    val dstream = rawData.map(_.value())

    import spark.implicits._

    dstream.foreachRDD(rdd => {

      val data = spark.createDataset(rdd)

      // write to CSV
      data.write
        .mode(SaveMode.Append)
        .csv("/user/hive/warehouse/my_database.db/my_table/")

      // write to snappy-compressed ORC format
      data.write
        .option("orc.compress", "snappy")
        .mode(SaveMode.Append)
        .orc("/user/hive/warehouse/my_database.db/my_table/")

      // write to Parquet
      data.write
        .mode(SaveMode.Append)
        .parquet("/user/hive/warehouse/my_database.db/my_table/")

    })



    // Allow the streaming context to start, and stick around until terminated
    ssc.start()
    ssc.awaitTermination()
  }
}
