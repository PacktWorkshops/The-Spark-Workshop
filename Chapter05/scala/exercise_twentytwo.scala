val spark = SparkSession
   .builder()
   .appName("exercise_twentytwo")
   .getOrCreate()

import spark.implicits._
import org.apache.spark.sql.types.DateType

val new_df = df.withColumn("registration_dttm", $"registration_dttm".cast(DateType))
new_df.select("registration_dttm", "first_name", "last_name").show(3)

new_df.printSchema
