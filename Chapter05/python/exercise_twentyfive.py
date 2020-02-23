spark = SparkSession \
   .builder \
   .appName("exercise_twentyfive") \
   .getOrCreate()


df.groupBy("gender").count().show()

df.groupby(df["gender"]).avg().show()

df.groupby(df.gender).min().show()
