package com.packtpub.spark.Exercise11_04

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.col

object Exercise11_04 {

  def main(args: Array[String]): Unit = {

    // Build a SparkSession in Local Mode
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("My Spark App")
      .getOrCreate()

    val categorized_animals = Seq(Row("dog", "pet"),
                                  Row("cat", "pet"),
                                  Row("bear", "wild"))

    val animal_foods = Seq(Row("dog", "kibble"),
                                Row("cat", "canned tuna"),
                                Row("bear", "salmon"))

    // Establish schema for the animals data
    val schema_Animals = List(
      StructField("name", StringType, nullable = true),
      StructField("category", StringType, nullable = true)
    )

    // Establish schema for the animal food data
    val schema_WAE = List(
      StructField("animal", StringType, nullable = true),
      StructField("food", StringType, nullable = true)
    )

    // Turn Raw Data into RDDs
    val animalDataRDD = spark.sparkContext.parallelize(categorized_animals)
    val animalFoodRDD = spark.sparkContext.parallelize(animal_foods)

    // Turn RDDs into DataFrames using schemas
    val animalData = spark.createDataFrame(animalDataRDD, StructType(schema_Animals))
    val animalFoods = spark.createDataFrame(animalFoodRDD, StructType(schema_WAE))

    // Join them together using the name of the animal as the join key
//    val animals_enhanced = animalData.join(animalFoods, usingColumn = "name")
    animalData.join(animalData, usingColumns = Seq("name", "category"))
    val animals_enhanced = animalData.join(animalFoods,
                                            joinExprs = col(colName = "name") === col(colName = "animal"),
                                            joinType = "left")

    animals_enhanced.show()

    animals_enhanced.foreach(record => {
      val name = record.getString(0)
      val category = record.getString(1)
      val food = record.getString(3)

      println(s"The ${name}, in the ${category} category, eats ${food} most commonly.")
    })
  }

}
