val spark = SparkSession
   .builder()
   .appName("exercise_twentyone")
   .getOrCreate()

import org.apache.spark.sql.functions.{col, lower, upper}

df.withColumn("last_name_lower", lower(col("last_name"))).select("id", "last_name", "last_name_lower").show(3)
