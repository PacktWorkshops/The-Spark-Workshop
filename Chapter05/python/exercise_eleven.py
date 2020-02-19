spark = SparkSession \
   .builder \
   .appName("exercise_eleven") \
   .getOrCreate()

df1 = spark.read.parquet("hdfs://user/your_user_name/data/userdata1.parquet")

df2 = spark.read.format("parquet").load("hdfs://user/your_user_name/data/userdata1.parquet")

df1.show()
