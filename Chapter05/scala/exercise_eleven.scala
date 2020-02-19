val spark = SparkSession
   .builder()
   .appName("exercise_eleven")
   .getOrCreate()

// Creating DataFrames from Parquet Files

val df1 = spark.read.parquet("hdfs://user/your_user_name/data/userdata1.parquet")

val df2 = spark.read.format("parquet").load("hdfs://user/your_user_name/data/userdata1.parquet")
