# The Spark Workshop

# Chapter 5
# DataFrames with Spark

# By Craig Covey
spark = SparkSession \
   .builder \
   .appName("creating_dataframes_from files") \
   .getOrCreate()

# Exercise 5.11

df1 = spark.read.parquet("hdfs://user/your_user_name/data/userdata1.parquet")

df2 = spark.read.format("parquet").load("hdfs://user/your_user_name/data/userdata1.parquet")

# Creating DataFrames from ORC Files

df3 = spark.read.orc("hdfs://user/your_user_name/data/orc/userdata1_orc")

df4 = spark.read.format("orc").load("hdfs://user/your_user_name/data/orc/userdata1_orc")

df5 = spark.read.orc("hdfs://user/your_user_name/data/orc")

df6 = spark.read.format("orc").load("hdfs://user/your_user_name/data/orc/")

# Creating DataFrames from JSON

df7 = spark.read.json("hdfs://user/your_user_name/data/books1.json")

df8 = spark.read.format("json").load("hdfs://user/your_user_name/data/books1.json")

# Creating DataFrames from Avro Files

df9 = spark.read.format("avro").load("hdfs://user/your_user_name/data/userdata1.avro")

df10 = spark.read.format("avro").load("hdfs://user/your_user_name/data/*.avro")

df11 = spark.read.foramt("avro").load("hdfs://user/your_user_name/data/userdata1.avro,hdfs://user/data/userdata5.avro,hdfs://user/your_user_name/data/userdata8.avro")

# Exercise 5.11

csv_pyspark_df1 = spark.read.csv("hdfs://user/your_user_name/data/csv/userdata1.csv"
  , sep=","
  , inferSchema="true"
  , header="true")

csv_pyspark_df2 = spark.read.format("csv").load("hdfs://user/your_user_name/data/csv/userdata1.csv"
  , sep=","
  , inferSchema="true"
  , header="true")

csv_pyspark_df3 = spark.read.load("hdfs://user/your_user_name/data/csv/userdata1.csv"
  , format="csv"
  , sep=","
  , inferSchema="true"
  , header="true")
