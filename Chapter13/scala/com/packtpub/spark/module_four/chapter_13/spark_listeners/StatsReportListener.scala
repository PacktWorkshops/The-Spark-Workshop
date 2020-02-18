package com.packtpub.spark.module_four.chapter_13.spark_listeners

import org.apache.spark.scheduler._
import org.apache.spark.sql.SparkSession

object StatsReportListener {

  def main(args: Array[String]): Unit = {

    // Build a SparkSession in Local Mode
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("My Spark App")
      .getOrCreate()

    // Used for turning RDD to DataSet
    import spark.implicits._

    // Add StatsReportListener
    spark.sparkContext.addSparkListener(new StatsReportListener)

    val animals = Seq("dog", "cat", "bear")
    val animalData = spark.sparkContext.parallelize(animals)

    val scaryOrNot = animalData.map(animal => {
      if (animal.equalsIgnoreCase("bear")){
        "scary"
      }
      else{
        "not scary"
      }
    })

    val counted = spark.createDataset(scaryOrNot).groupBy("value").count()
    counted.show()

    Thread.sleep(100000)
  }
}
