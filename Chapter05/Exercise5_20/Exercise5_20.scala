val spark = SparkSession
   .builder()
   .appName("exercise_twenty")
   .getOrCreate()

import spark.implicits._

df.withColumn("monthly_salary", $"salary" / 12).select("first_name", "salary", "monthly_salary").show(5)

import org.apache.spark.sql.functions.round

df.withColumn("monthly_salary", round($"salary" / 12, 2))
  .select("first_name", "salary", "monthly_salary")
  .show(5)
