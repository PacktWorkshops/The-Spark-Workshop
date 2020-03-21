val spark = SparkSession
   .builder()
   .appName("exercise_twentyseven")
   .getOrCreate()

import spark.implicits._
import org.apache.spark.sql.functions.{avg, min, max, sum}

df.groupBy("gender").agg(avg(df("salary")), min(df("salary")), max($"salary"), sum($"salary")).show()
