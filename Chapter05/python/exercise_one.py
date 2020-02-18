spark = SparkSession \
   .builder \
   .appName("exercise_one") \
   .getOrCreate()

hadoop_list = [[1, "MapReduce"], [2, "YARN"], [3, "Hive"], [4, "Pig"], [5, "Spark"], [6, "Zookeeper"]]

hadoop_df = spark.createDataFrame(hadoop_list)

hadoop_df.show()
hadoop_df.printSchema()
