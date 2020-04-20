package com.packtpub.spark.module_four.chapter_13

import com.packtpub.spark.module_four.chapter_13.spark_listeners.WorkshopBatchListener
import org.apache.spark.sql.SparkSession

object ActivityOne {

  def main(args: Array[String]): Unit = {

    // Build a SparkSession in Local Mode
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("My Spark App")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.addSparkListener(new WorkshopBatchListener)

    val csvDF = spark
      .read
      .option("inferSchema", "true")
      .format("csv")
      .option("header", "true")
      .load("src/main/resources/data/chapter_13/activity_one/Motor_Vehicle_Collisions_-_Person.csv")

    csvDF.cache()

    // Count of Accidents by Person Type
    val accidentsByType = csvDF
      .groupBy("PERSON_TYPE")
      .count()
      .withColumnRenamed("count", "numAccidents")
      .orderBy($"numAccidents".desc)

    accidentsByType.show(truncate = false)
    accidentsByType.coalesce(1).write.csv("accidentsByType")

    // Count of Accidents by Person Type
    val averageAges = csvDF
      .groupBy("PERSON_TYPE")
      .avg("PERSON_AGE")

    averageAges.show(truncate = false)
    averageAges.coalesce(1).write.csv("averageAgeOfPersons")
  }
}
