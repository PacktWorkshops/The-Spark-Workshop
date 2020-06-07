import org.apache.spark.sql.functions.{max, min}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object Exercise11_05 {

  def main(args: Array[String]): Unit = {

    // Build a SparkSession in Local Mode
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("My Spark App")
      .getOrCreate()

    val my_previous_pets = Seq(Row("fido", "dog", 4, "brown"),
      Row("annabelle", "cat", 15, "white"),
      Row("fred", "bear", 29, "brown"),
      Row("gus", "parakeet", 2, "black"),
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

    // how old is the oldest cat?
    petsDF.where("type = 'cat'").agg(Map(
      "age" -> "max"
    )).show()

    // what about youngest and oldest cat ages?
    petsDF.where("type = 'cat'")
      .groupBy("type")
      .agg(min("age").alias("min_age"),
        max("age").alias("max_age"))
      .show()

     // what is the average age of dogs in the set?
    petsDF.where("type = 'dog'").groupBy("type").agg("age" -> "avg").show()
    
    // how many pets are for each color?
    petsDF.groupBy("color").count().show()

    // what are the 3 most popular pet names? (using temp view instead of functional approach)
    spark.sql("select nickname, count(*) as occurrences from pets group by nickname order by occurrences desc limit 3").show()

  }

}
