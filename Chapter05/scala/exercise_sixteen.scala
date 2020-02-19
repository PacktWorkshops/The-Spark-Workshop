val spark = SparkSession
   .builder()
   .appName("exercise_sixteen")
   .getOrCreate()

import spark.implicits._
df.filter($"salary" > 10000).filter($"gender" === "Female").show()

df.filter($"salary" > 150000 && $"country" === "United States").count()

df.filter($"salary" > 150000 || $"country" === "United States").count()
