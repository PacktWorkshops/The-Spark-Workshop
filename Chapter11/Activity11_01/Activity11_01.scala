import org.apache.spark.sql.SparkSession

object Activity11_01 {

case class BabyNameData(birth_year: String,
                        gender: String,
                        ethnicity: String,
                        name: String,
                        count: String,
                        rank: String)

  def main(args: Array[String]): Unit = {

    // Build a SparkSession in Local Mode
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("My Spark App")
      .enableHiveSupport()
      .getOrCreate()

    // Import Spark Implicits for using .as[] function
    import spark.implicits._

    // you can use a DataSet
    val babyNames = spark
      .read
      .option("header", "true")
      .csv("src/main/resources/data/Popular_Baby_Names.csv")
      .withColumnRenamed("Year of Birth", "birth_year")
      .withColumnRenamed("Gender", "gender")
      .withColumnRenamed("Ethnicity", "ethnicity")
      .withColumnRenamed("Child's First Name", "name")
      .withColumnRenamed("Count", "count")
      .withColumnRenamed("Rank", "rank")
      .as[BabyNameData]

    // or you can use a DataFrame
    val babyNames = spark
      .read
      .option("header", "true")
      .csv("src/main/resources/data/Popular_Baby_Names.csv")
      .withColumnRenamed("Year of Birth", "birth_year")
      .withColumnRenamed("Gender", "gender")
      .withColumnRenamed("Ethnicity", "ethnicity")
      .withColumnRenamed("Child's First Name", "name")
      .withColumnRenamed("Count", "count")
      .withColumnRenamed("Rank", "rank")

    babyNames.createOrReplaceTempView("babies")
    
    spark.sql("select first(name) as name, " +
      "first(gender) as gender, " +
      "ethnicity as ethnicity, " +
      "min(rank) as rank " +
      "from babies " +
      "where birth_year=2016 " +
      "group by ethnicity, gender " +
      "order by ethnicity, gender")
    .show(truncate = false)


  }
}
