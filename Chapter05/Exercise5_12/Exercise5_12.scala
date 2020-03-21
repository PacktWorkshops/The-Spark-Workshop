val spark = SparkSession
   .builder()
   .appName("exercise_twelve")
   .getOrCreate()

val csv_scala_df1 = spark.read
  .option("sep", ",")
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("hdfs://user/your_user_name/data/csv/userdata1.csv")

val csv_scala_df2 = spark.read
  .format("csv")
  .option("sep", ",")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("hdfs://user/your_user_name/data/csv/userdata1.csv")
