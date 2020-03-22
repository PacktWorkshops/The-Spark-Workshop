spark = SparkSession \
   .builder \
   .appName("exercise_twelve") \
   .getOrCreate()

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
