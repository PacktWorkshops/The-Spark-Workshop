from pyspark.sql import SparkSession
from pyspark.sql import Row
import string

# Create a Spark Session
spark = SparkSession \
    .builder \
    .appName("My Spark App") \
    .master("local[2]") \
    .getOrCreate()

# create spark contexts
sc = spark.sparkContext

babyNames = spark.read\
    .option("header", "true")\
    .csv("Popular_Baby_Names.csv")\
    .withColumnRenamed("Year of Birth", "birth_year")\
    .withColumnRenamed("Gender", "gender")\
    .withColumnRenamed("Ethnicity", "ethnicity")\
    .withColumnRenamed("Child's First Name", "name")\
    .withColumnRenamed("Count", "count")\
    .withColumnRenamed("Rank", "rank")

babyNames.registerTempTable("babies")

spark.sql("select first(name) as name, " +
          "first(gender) as gender, " +
          "ethnicity as ethnicity, " +
          "min(rank) as rank " +
          "from babies " +
          "where birth_year=2016 " +
          "group by ethnicity, gender " +
          "order by ethnicity, gender").show()
