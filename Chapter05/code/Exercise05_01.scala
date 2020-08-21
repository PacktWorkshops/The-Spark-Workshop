import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().master("local[2]").appName("My Spark App").getOrCreate()
