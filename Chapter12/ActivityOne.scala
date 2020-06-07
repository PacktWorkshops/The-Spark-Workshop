import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object ActivityOne {

  def main(args: Array[String]): Unit = {

    // Build a SparkSession in Local Mode
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("My Spark App")
      .getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val schema = new StructType()
      .add("conversationTitle", "string")
      .add("tapeNumber", "string")
      .add("conversationNumber", "string")
      .add("identifier", "string")
      .add("startDateTime", "string")
      .add("endDateTime", "string")
      .add("startDate", "string")
      .add("startTime", "string")
      .add("endDate", "string")
      .add("endTime", "string")
      .add("dateCertainty", "string")
      .add("timeCertainty", "string")
      .add("participants", "string")
      .add("description", "string")
      .add("locationCode", "string")
      .add("recordingDevice", "string")
      .add("latitudeEstimated", "string")
      .add("longitudeEstimated", "string")
      .add("collection", "string")
      .add("collectionURL", "string")
      .add("chronCode", "string")
      .add("chronRelease", "string")
      .add("chronReleaseDate", "string")
      .add("aogpRelease", "string")
      .add("aogpSegment", "string")
      .add("digitalAccess", "string")
      .add("physicalAccess", "string")
      .add("contactEmail", "string")
      .add("lastModified", "string")

    val csvDF = spark.readStream
      .schema(schema)
      .option("header", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .csv("src/main/resources/data/chapter_12/activity_one/")

    val cleaned = csvDF.filter("participants is not null").flatMap(x => {
      x.getString(x.fieldIndex("participants")).split(";")
    }).map(x => {
      x.trim()
    }).withColumnRenamed("value", "participant")

    val callCounts = cleaned.groupBy("participant").count().orderBy($"count".desc)

    // Start running the query that prints the running counts to the console
    val query = callCounts.writeStream
      .outputMode("complete").option("truncate", "false")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
