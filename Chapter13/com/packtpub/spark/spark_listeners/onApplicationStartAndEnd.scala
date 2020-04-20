package com.packtpub.spark.module_four.chapter_13.spark_listeners

import org.apache.spark.sql.SparkSession

object onApplicationStartAndEnd {

  def main(args: Array[String]): Unit = {

    // Build a SparkSession in Local Mode
    val spark = SparkSession
      .builder()
      // Using .config() below, we can add our custom listener before Spark is established
      // netting us access to onApplicationStart, unlike using the .addSparkListener method
      .config("spark.extraListeners", "com.packtpub.spark.module_four.chapter_13.spark_listeners.WorkshopBatchListener")
      .master("local[2]")
      .appName("My Spark App")
      .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")

    // If uncommented, this would not provide us access to onApplicationStart
    // since it would be added after the Spark Context is already established
    // and the application already started.
    // Add StatsReportListener
    // spark.sparkContext.addSparkListener(new WorkshopBatchListener)

    // Used for turning RDD to DataSet
    import spark.implicits._

    val animals = Seq("dog", "cat", "bear")
    val animalData = spark.sparkContext.parallelize(animals)

    val counted = spark.createDataset(animalData).groupBy("value").count()
    counted.show()
  }
}
