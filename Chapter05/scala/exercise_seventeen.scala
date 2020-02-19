val spark = SparkSession
   .builder()
   .appName("exercise_seventeen")
   .getOrCreate()

import spark.implicits._

df.orderBy($"birthdate".desc).show()

df.orderBy(df("birthdate").asc).show()

df.orderBy(df("birthdate").desc, df("country".desc)).show()

df.orderBy($"birthdate".desc, $"country".desc).show()
