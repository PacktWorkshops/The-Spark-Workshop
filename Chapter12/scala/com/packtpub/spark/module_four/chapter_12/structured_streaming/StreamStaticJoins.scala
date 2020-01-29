package com.packtpub.spark.module_four.chapter_12.structured_streaming

import org.apache.spark.sql.SparkSession

object StreamStaticJoins {

  case class WebTraffic(userID: String, actionID: Int)
  case class Action(actionID: Int, description: String)

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

    // Establish Lookup Data for web traffic
    val actions = Seq((1, "opened website"), (2, "clicked"), (3, "scrolled"), (4, "left website"))
    val webActions =  spark.createDataset(actions).map(x => {
      Action(x._1, x._2)
    })

    // Make a DataFrame, `lines`, that will store every entry received from Netcat
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    // Every line is just a comma-separated string received from the stream, so turn it into Dataset
    val webTrafficRaw = lines.as[String]

    val webTraffic = webTrafficRaw.map(x => {
      val parts = x.split(",")
      val userID = parts{0}
      val actionID = parts{1}.toInt
      WebTraffic(userID, actionID)
    }).join(webActions, "actionID")

    // Generate running word count
    val actionCounts = webTraffic.groupBy("description").count()

    // Start running the query that prints the running counts to the console
    val query = actionCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
