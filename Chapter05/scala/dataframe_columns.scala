// The Spark Workshop

// Chapter 5
// DataFrames with Spark

// By Craig Covey

df.columns

df.columns(1)
df.columns.slice(1, 4)

df.dtypes

// Exercise 5.19

val reg_df = df.select(df("registration_dttm"))
reg_df.show(3)
reg_df.printSchema

val long_reg_df = df.select($"registration_dttm".cast("long"))
long_reg_df.show(3)
long_reg_df.printSchema



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

// Exercise 5.20

df.withColumn("monthly_salary", $"salary" / 12).select("first_name", "salary", "monthly_salary").show(5)

import org.apache.spark.sql.functions.round

df.withColumn("monthly_salary", round($"salary" / 12, 2))
  .select("first_name", "salary", "monthly_salary")
  .show(5)



import org.apache.spark.sql.functions.lit
df.withColumn("constant_value", lit(4)).show(5)

// Exercise 5.21

import spark.implicits._
import org.apache.spark.sql.functions.{col, lower, upper}

df.withColumn("last_name_lower", lower(col("last_name"))).select("id", "last_name", "last_name_lower").show(3)

// Exercise 5.22

import spark.implicits._
import org.apache.spark.sql.types.DateType
val new_df = df.withColumn("registration_dttm", $"registration_dttm".cast(DateType))
new_df.select("registration_dttm", "first_name", "last_name").show(3)
new_df.printSchema



import spark.implicits._
df.withColumn("monthly_salary", $"salary" / 12).select("first_name", "salary", "monthly_salary").show(5)
