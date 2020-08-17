val spark = SparkSession
   .builder()
   .appName("exercise_thirteen")
   .getOrCreate()

df.head(2)

df.take(2)
