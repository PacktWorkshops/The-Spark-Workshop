spark = SparkSession \
   .builder \
   .appName("exercise_twenty") \
   .getOrCreate()

df.withColumn("append_id", df["id"] + 10).select("id", "append_id").show(5)
