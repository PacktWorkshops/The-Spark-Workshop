import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object Exercise11_10 {

  def main(args: Array[String]): Unit = {

    // Build a SparkSession in Local Mode
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("My Spark App")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // an example of a normalized dataset
    val animalsNormalized = Seq(Row("fido", 1, 4, 1),
                                      Row("annabelle", 2, 15, 2),
                                      Row("fred", 3, 29, 1),
                                      Row("fred", 4, 1, 1),
                                      Row("gus", 5, 1, 4),
                                      Row("daisy", 6, 2, 5))

    // lookup table for animal type
    val animalTypeLookup = Seq(Row("dog", 1),
                       Row("cat", 2),
                       Row("bear", 3),
                       Row("parrot",4),
                       Row("fish", 5),
                       Row("iguana",6))

    // lookup table for animal color
    val animalColorLookup = Seq(Row("brown", 1),
                        Row("white", 2),
                        Row("black", 3),
                        Row("gold",4),
                        Row("green", 5),
                        Row("red",6))

    val schemaNormalized = List(
      StructField("nickname", StringType, nullable = true),
      StructField("type", IntegerType, nullable = true),
      StructField("age", IntegerType, nullable = true),
      StructField("color", IntegerType, nullable = true)
    )

    val schemaColor = List(
      StructField("color_name", StringType, nullable = true),
      StructField("color_id", IntegerType, nullable = true)
    )

    val schemaType = List(
      StructField("type_name", StringType, nullable = true),
      StructField("type_id", IntegerType, nullable = true)
    )

    // create RDDs
    val petsRDD = spark.sparkContext.parallelize(animalsNormalized)
    val colorsRDD = spark.sparkContext.parallelize(animalColorLookup)
    val typesRDD = spark.sparkContext.parallelize(animalTypeLookup)

    // create DataFrames
    val petsDF = spark.createDataFrame(petsRDD, StructType(schemaNormalized))
    val colors = spark.createDataFrame(colorsRDD, StructType(schemaColor))
    val types = spark.createDataFrame(typesRDD, StructType(schemaType))

    val petsWithColors = petsDF.join(colors, col("color") === col("color_id"), "left")
    petsWithColors.select("nickname","color_name", "age").show()

    val petsWithColorAndType = petsWithColors.join(types, col("type") === col("type_id"), "left")
    petsWithColorAndType.select("nickname","type_name", "age", "color_name").show()
  }

}
