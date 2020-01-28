// The Spark Workshop

// Chapter 5
// DataFrames with Spark

// By Craig Covey

// Exercise 5.23

df.describe().show()

df.describe("last_name", "salary").show()

df.select("salary").summary().show()

df.select("id", "salary").summary("mean", "stddev", "15%", "66%").show()



df.stat.crosstab("country", "gender").show(10)


// Exercise 5.24

df.groupBy().avg().show()

df.groupBy().sum().show()

df.groupBy().max("salary").show()


// Exercise 5.25

df.groupBy("gender").count().show()

import spark.implicits._
df.groupBy($"gender").max().show()


// Exercise 5.26

df.groupBy("gender").avg("salary").show()

df.groupBy("country").avg("salary", "id").show(7)


// The Aggregate Method

import spark.implicits._
import org.apache.spark.sql.functions.max
df.groupBy("country").agg(max($"salary")).show(5)

import org.apache.spark.sql.functions.min
df.groupBy("country").agg(min(df("salary"))).show(5)


import spark.implicits._
import org.apache.spark.sql.functions.max
df.groupBy("country").agg(max($"salary")).show(5)


// Exercise 5.27

import spark.implicits._
import org.apache.spark.sql.functions.{avg, min, max, sum}

df.groupBy("gender").agg(avg(df("salary")), min(df("salary")), max($"salary"), sum($"salary")).show()


// Other agg() Options

df.groupBy("gender").agg(Map("salary" -> "max", "salary" -> "avg")).show()

df.groupBy("gender").agg(Map("salary" -> "max", "id" -> "avg")).show()


// Exercise 5.28

df.groupby("gender", "country").avg().show(5)

import spark.implicits._
import org.apache.spark.sql.functions.{max, min}
df.groupBy("country", "gender").agg(max(df("salary")), min($"salary"))
  .orderBy($"country".desc)
  .show(7)

// Renaming Aggregate Columns

import org.apache.spark.sql.functions.{avg, max}
df.groupBy("country").agg(avg(df("salary")).alias("avg_income"), max(df("salary")).alias("max_money")).show(7)

import org.apache.spark.sql.functions.{avg, max}

df.groupBy("country").agg(avg(df("salary")), max(df("salary")))
  .withColumnRenamed("avg(salary)", "avg_income")
  .withColumnRenamed("max(salary)", "max_money")
  .show(7)
