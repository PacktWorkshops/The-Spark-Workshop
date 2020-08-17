val spark = SparkSession
   .builder()
   .appName("creating_dataframes_from_files")
   .getOrCreate()

// Creating DataFrames from Parquet Files

val df1 = spark.read.parquet("hdfs://user/your_user_name/data/userdata1.parquet")

val df2 = spark.read.format("parquet").load("hdfs://user/your_user_name/data/userdata1.parquet")

// Creating DataFrames from ORC Files

val df3 = spark.read.orc("hdfs://user/your_user_name/data/orc/userdata1_orc")

val df4 = spark.read.format("orc").load("hdfs://user/your_user_name/data/orc/userdata1_orc")

val df5 = spark.read.orc("hdfs://user/your_user_name/data/orc")

val df6 = spark.read.format("orc").load("hdfs://user/your_user_name/data/orc/")

// Creating DataFrames from JSON Files

val df7 = spark.read.json("hdfs://user/your_user_name/data/books1.json")

val df8 = spark.read.format("json").load("hdfs://user/your_user_name/data/books1.json")

// Creating DataFrames from Avro Files

val df9 = spark.read.format("avro").load("hdfs://user/your_user_name/data/userdata1.avro")

val df10 = spark.read.format("avro").load("hdfs://user/your_user_name/data/*.avro")

val df11 = spark.read.foramt("avro").load("hdfs://user/your_user_name/data/userdata1.avro,hdfs://user/data/userdata5.avro,hdfs://user/your_user_name/data/userdata8.avro")

// Creating DataFrames from CSV Files

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
