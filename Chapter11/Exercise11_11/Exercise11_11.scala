import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object Exercise11_11 {

  def main(args: Array[String]): Unit = {

    // Build a SparkSession in Local Mode
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("My Spark App")
      .getOrCreate()

    import spark.implicits._

    // an example of a denormalized dataset
    val animalsDenormalized = Seq(Row("fido", "dog", 4, "brown"),
                                   Row("annabelle", "cat", 15, "white"),
                                   Row("fred", "bear", 29, "brown"),
                                   Row("fred", "parrot", 1, "brown"),
                                   Row("gus", "fish", 1, "gold"),
                                   Row("daisy", "iguana", 2, "green"))

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

    val schemaDenormalized = List(
      StructField("nickname", StringType, nullable = true),
      StructField("type", StringType, nullable = true),
      StructField("age", IntegerType, nullable = true),
      StructField("color", StringType, nullable = true)
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
    val petsRDD = spark.sparkContext.parallelize(animalsDenormalized)
    val colorsRDD = spark.sparkContext.parallelize(animalColorLookup)
    val typesRDD = spark.sparkContext.parallelize(animalTypeLookup)

    // create DataFrames
    val petsDF = spark.createDataFrame(petsRDD, StructType(schemaDenormalized))
    val colors = spark.createDataFrame(colorsRDD, StructType(schemaColor))
    val types = spark.createDataFrame(typesRDD, StructType(schemaType))

    val petsWithColors = petsDF.join(colors, col("color") === col("color_name"), "left")
    petsWithColors.select("nickname","color_id", "age").show()

    val petsWithColorAndType = petsWithColors.join(types, col("type") === col("type_name"), "left")
    petsWithColorAndType.select("nickname","type_id", "age", "color_id").show()
  }

}
