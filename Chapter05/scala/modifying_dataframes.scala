// The Spark Workshop

// Chapter 5
// DataFrames with Spark

// By Craig Covey

val spark = SparkSession
   .builder()
   .appName("modifying_dataframes")
   .getOrCreate()


// Exercise 5.13

df.head(2)
df.take(2)

// Selecting Columns of a DataFrame

df.select("first_name", "last_name").show(5)

df.select("last_name", "first_name").show(5)

df.select("*").show()

df.select(df("first_name"), df("country"), df("salary").show()

import spark.implicits._
df.select($"id", $"gender", $"birthdate").show()

df.select(df("first_name"), $"country", $"salary").show()

// Exercise 5.14

df.select("gender").distinct().show()

df.select("country").distinct.show(5)

// Exercise 5.15

import spark.implicits._
df.filter($"first_name" === "Albert").count()

// Exercise 5.16

import spark.implicits._
df.filter($"salary" > 10000).filter($"gender" === "Female").show()

df.filter($"salary" > 150000 && $"country" === "United States").count()
df.filter($"salary" > 150000 || $"country" === "United States").count()

// Exercise 5.17

df.orderBy($"birthdate".desc).show()
df.orderBy(df("birthdate").asc).show()

df.orderBy(df("birthdate").desc, df("country".desc)).show()
df.orderBy($"birthdate".desc, $"country".desc).show()

// Exercise 5.18

df.select("id", "first_name", "last_name", "gender", "country", "birthdate", "salary")
  .filter(df("country") === "United States")
  .orderBy(df("gender").asc, df("salary").desc)
  .show()
