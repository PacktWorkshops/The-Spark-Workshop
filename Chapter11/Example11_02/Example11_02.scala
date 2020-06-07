import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object Example11_02 {

  def main(args: Array[String]): Unit = {

    // Build a SparkSession in Local Mode
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("My Spark App")
      .getOrCreate()

    // create a dataset of animals with id, type and color
    val clients = Seq(Row(1, "dog", "brown", 1),
                  Row(1, "dog", "brown", 2),
                  Row(3, "dog", "white", 6),
                  Row(3, "dog", "white", 8),
                  Row(4, "dog", "black", 4),
                  Row(4, "dog", "black", 14),
                  Row(5, "dog", "red", 11),
                  Row(6, "dog", "gold", 9),
                  Row(6, "dog", "gold", 5),
                  Row(7, "dog", "spotted", 7),
                  Row(8, "dog", "brown", 1),
                  Row(9, "dog", "brown", 20),
                  Row(10, "dog", "brown", 3),
                  Row(11, "dog", "brown", 7),
                  Row(12, "dog", "brown", 9),
                  Row(13, "dog", "brown", 10),
                  Row(14, "dog", "brown", 3),
                  Row(15, "dog", "brown", 6),
                  Row(16, "dog", "brown", 13),
                  Row(17, "dog", "brown", 4),
                  Row(18, "dog", "brown", 5),
                  Row(19, "dog", "brown", 7),
                  Row(20, "dog", "brown", 8))

    val schema = List(
      StructField("id", IntegerType, nullable = true),
      StructField("type", StringType, nullable = true),
      StructField("color", StringType, nullable = true),
      StructField("age", IntegerType, nullable = true))

    // create RDD
    val clientsRDD = spark.sparkContext.parallelize(clients)

    // create DataFrame
    val clientsDF = spark.createDataFrame(clientsRDD, StructType(schema))

    clientsDF.createOrReplaceTempView("dogs")

// slower due to skew
spark.sql("select avg(age) as average_age, " +
  "color " +
  "from dogs " +
  "group by color " +
  "order by average_age desc").show()
  }

}
