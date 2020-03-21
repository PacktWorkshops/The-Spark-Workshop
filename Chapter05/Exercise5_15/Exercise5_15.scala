val spark = SparkSession
   .builder()
   .appName("exercise_fifteen")
   .getOrCreate()

import spark.implicits._

df.filter($"first_name" === "Albert").count()
