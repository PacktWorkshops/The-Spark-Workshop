import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Exercise11_08 {

  def main(args: Array[String]): Unit = {

    // Build a SparkSession in Local Mode
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("My Spark App")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    val my_previous_pets = Seq(Row("fido", "dog", 4, "brown"),
                               Row("annabelle", "cat", 15, "white"),
                               Row("fred", "bear", 29, "brown"),
                               Row("daisy", "cat", 8, "black"),
                               Row("jerry", "cat", 1, "white"),
                               Row("fred", "parrot", 1, "brown"),
                               Row("gus", "fish", 1, "gold"),
                               Row("gus", "dog", 11, "black"),
                               Row("daisy", "iguana", 2, "green"),
                               Row("rufus", "dog", 10, "gold"))

    val schema = List(
      StructField("nickname", StringType, nullable = true),
      StructField("type", StringType, nullable = true),
      StructField("age", IntegerType, nullable = true),
      StructField("color", StringType, nullable = true)
    )

    val petsRDD = spark.sparkContext.parallelize(my_previous_pets)
    val petsDF = spark.createDataFrame(petsRDD, StructType(schema))

    petsDF.createOrReplaceTempView("pets")

    // establish window that partitions by a group, and orders by metric for ranking by
    val window = Window.partitionBy("type").orderBy($"age".desc)

    // rank the dogs from oldest to youngest
    petsDF.withColumn("row_number", row_number().over(window))
          .withColumnRenamed("row_number", "rank")
          .where("rank <= 2 and (type = 'dog' or type = 'cat')")
          .orderBy("type", "nickname")
          .show()
  }

}
