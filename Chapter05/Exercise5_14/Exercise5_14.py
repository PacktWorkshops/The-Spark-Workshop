val spark = SparkSession
   .builder()
   .appName("exercise_fourteen")
   .getOrCreate()

df.select("gender").distinct().show()

df.select("country").distinct.show(5)
