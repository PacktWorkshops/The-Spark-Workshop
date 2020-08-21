spark = SparkSession \
   .builder \
   .appName("exercise_twentysix") \
   .getOrCreate()

df.groupBy("gender").avg("salary").show()

df.groupBy("country").avg("salary", "id").show(7)
