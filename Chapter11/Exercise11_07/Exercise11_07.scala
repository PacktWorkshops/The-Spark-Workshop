import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object Exercise11_07 {

  def main(args: Array[String]): Unit = {

    // Build a SparkSession in Local Mode
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("My Spark App")
      .getOrCreate()

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

    // option 1: pure sql
    spark.sql("select nickname as youngest_cat, min(age) as age from pets where type = 'cat' group by nickname order by age asc limit 1").show()
    spark.sql("select nickname as oldest_cat, max(age) as age from pets where type = 'cat' group by nickname order by age desc limit 1").show()

    // option 2: functional
    petsDF.where("type = 'cat'").sort("age").limit(1).show()
    petsDF.where("type = 'cat'").sort($"age".desc).limit(1).show()

  }

}
