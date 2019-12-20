package com.packtpub.spark.module_four.chapter_11

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object Deduplication {

  def main(args: Array[String]): Unit = {

    // Build a SparkSession in Local Mode
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("My Spark App")
      .getOrCreate()

    val categorized_animals = Seq(Row("dog", "pet"),
                                  Row("cat", "pet"),
                                  Row("bear", "wild"),
                                  Row("cat", "pet"),
                                  Row("cat", "pet"))

    val schema = List(
      StructField("name", StringType, nullable = true),
      StructField("category", StringType, nullable = true)
    )

    val animalDataRDD = spark.sparkContext.parallelize(categorized_animals)

    val animalData = spark.createDataFrame(animalDataRDD, StructType(schema))

    animalData.show()
    val deduped = animalData.dropDuplicates()
    deduped.show()

  }

}
