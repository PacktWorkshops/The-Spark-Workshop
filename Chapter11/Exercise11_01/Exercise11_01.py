from pyspark.sql import SparkSession

spark = SparkSession\
    .builder\
    .appName("My Spark App")\
    .master("local[2]")\
    .getOrCreate()