val spark = SparkSession
   .builder()
   .appName("dataframe_aggregations")
   .getOrCreate()

// The Aggregate Method

import spark.implicits._
import org.apache.spark.sql.functions.max
df.groupBy("country").agg(max($"salary")).show(5)

import org.apache.spark.sql.functions.min
df.groupBy("country").agg(min(df("salary"))).show(5)

import spark.implicits._
import org.apache.spark.sql.functions.max
df.groupBy("country").agg(max($"salary")).show(5)

// Other agg() Options

df.groupBy("gender").agg(Map("salary" -> "max", "salary" -> "avg")).show()

df.groupBy("gender").agg(Map("salary" -> "max", "id" -> "avg")).show()

// Renaming Aggregate Columns

import org.apache.spark.sql.functions.{avg, max}
df.groupBy("country").agg(avg(df("salary")).alias("avg_income"), max(df("salary")).alias("max_money")).show(7)

import org.apache.spark.sql.functions.{avg, max}

df.groupBy("country").agg(avg(df("salary")), max(df("salary")))
  .withColumnRenamed("avg(salary)", "avg_income")
  .withColumnRenamed("max(salary)", "max_money")
  .show(7)
