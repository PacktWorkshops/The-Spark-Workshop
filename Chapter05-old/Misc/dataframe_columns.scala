val spark = SparkSession
   .builder()
   .appName("dataframe_columns")
   .getOrCreate()

import spark.implicits._

df.columns

df.columns(1)
df.columns.slice(1, 4)

df.dtypes


df.select($"registration_dttm".cast("long").alias("seconds_since_1970")).show(5)

// Drop Columns

df.drop("ip_address")
df.drop("comments", "registration_dttm")

df.drop($"salary")
df.drop($"comments").drop($"salary")

// Rename Columns

println(df.columns.mkString(" "))
val df_renamed = df.withColumnRenamed("birthdate", "Date_of_Birth")
println(df_renamed.columns.mkString(" "))


import org.apache.spark.sql.functions.lit
df.withColumn("constant_value", lit(4)).show(5)


import spark.implicits._
df.withColumn("monthly_salary", $"salary" / 12).select("first_name", "salary", "monthly_salary").show(5)
