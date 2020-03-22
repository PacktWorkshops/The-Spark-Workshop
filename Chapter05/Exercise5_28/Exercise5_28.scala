val spark = SparkSession
   .builder()
   .appName("exercise_twentysix")
   .getOrCreate()


df.groupby("gender", "country").avg().show(5)

import spark.implicits._
import org.apache.spark.sql.functions.{max, min}
df.groupBy("country", "gender").agg(max(df("salary")), min($"salary"))
 .orderBy($"country".desc)
 .show(7)
