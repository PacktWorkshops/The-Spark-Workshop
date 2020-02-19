val spark = SparkSession
   .builder()
   .appName("exercise_fifteen")
   .getOrCreate()

df.filter(df.salary > 50000).show(4)

df.filter(df["salary"] > 50000).count()
