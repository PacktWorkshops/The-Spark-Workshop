import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object Example11_01 {

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
                      Row(7, "dog", "spotted", 7))

    val schema = List(
      StructField("id", IntegerType, nullable = true),
      StructField("type", StringType, nullable = true),
      StructField("color", StringType, nullable = true),
      StructField("age", IntegerType, nullable = true))

    // create RDD
    val clientsRDD = spark.sparkContext.parallelize(clients)

    // create DataFrame
    val clientsDF = spark.createDataFrame(clientsRDD, StructType(schema))

    // create temp view and query it     
    clientsDF.createOrReplaceTempView("dogs")
    spark.sql("select cast(avg(age) as INT) as average_age, color from dogs group by color order by average_age desc").show()
  }

}
