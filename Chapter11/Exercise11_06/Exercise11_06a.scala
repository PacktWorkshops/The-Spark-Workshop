import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object Exercise11_06a {

  def main(args: Array[String]): Unit = {

    // Build a SparkSession in Local Mode
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("My Spark App")
      .getOrCreate()

    import spark.implicits._
    val my_previous_pets = Seq(Row("annabelle", "cat"),
                               Row("daisy", "kitten"),
                               Row("roger", "puppy"),
                               Row("joe", "puppy dog"),
                               Row("rosco", "dog"),
                               Row("julie", "feline"))

    val schema = List(
      StructField("nickname", StringType, nullable = true),
      StructField("type", StringType, nullable = true)
    )

    val petsRDD = spark.sparkContext.parallelize(my_previous_pets)
    val petsDF = spark.createDataFrame(petsRDD, StructType(schema))

    val dogs = petsDF.where($"type".isin("dog", "puppy", "puppy dog", "hound", "canine"))
    val cats = petsDF.where($"type".isin ("cat", "kitty", "kitten", "feline", "kitty cat"))

    dogs.show()
    cats.show()
  }

}
