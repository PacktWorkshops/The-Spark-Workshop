package com.packtpub.spark.Exercise11_03

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object Exercise11_03 {

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

    // filter out any records where name column starts with "c"
    val nonCats = animalData.filter("name not like 'c%'")

    // or you can use the where method, which is an alias on the filter method
    val nonCatsTwo = animalData.where("name != 'cat'")
    nonCatsTwo.show()

    nonCats.foreach(animal => {
      println(s"I am a ${animal.get(0)}, which does not start with a 'c'.")
    })

    val nonPets = animalData.filter("category != 'pet'")

    nonPets.foreach(animal => {
      println(s"I am no pet, for I am a ${animal.get(1)} ${animal.get(0)}!")
    })

  }

}
