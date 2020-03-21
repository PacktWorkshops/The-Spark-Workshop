val spark = SparkSession
   .builder()
   .appName("exercise_twentyfive")
   .getOrCreate()


df.groupBy("gender").count().show()

import spark.implicits._
df.groupBy($"gender").max().show()
