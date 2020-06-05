import java.util.Properties

import org.apache.spark.sql.SparkSession

object Exercise11_12 {

  case class Saying(word: String)

  def main(args: Array[String]): Unit = {

    // Build a SparkSession in Local Mode
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("My Spark App")
      .enableHiveSupport()
      .getOrCreate()

    // Setup Database and Table Variables
    val database = "spark_workshop"
    val table = "books"
    val tbl = database + "." + table
    val host = "mysql.mycompany.com"
    val port = 3306

    // Set Driver Properties
    val driverProperties = new Properties
    driverProperties.put("user", "someUsername")
    driverProperties.put("password", "S0mePassw0rd")

    // Set any DB options you need
    val options = Map("jdbcCompliantTruncation" -> "false", "zeroDateTimeBehavior" -> "convertToNull")

    // build your JDBC Url
    val jdbcUrl = s"jdbc:mysql://$host" + ":" + port  +
      s"/$database?user=${driverProperties.getProperty("user")}&password=${driverProperties.getProperty("password")}" +
      options.map(x => s"&${x._1}=${x._2}").mkString("")

    // Read the MySQL Table into a DataFrame
    val results = spark.read.jdbc(jdbcUrl, s"$tbl", driverProperties)

    results.show()

    println("Table " + tbl + " ingested with " + results.count() + " records.")
  }
}