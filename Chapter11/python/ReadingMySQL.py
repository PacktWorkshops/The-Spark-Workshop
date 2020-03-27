

import pyspark
from pyspark.sql import SparkSession

# Create a Spark Session
spark = SparkSession \
    .builder \
    .appName("My Spark App") \
    .master("local[2]") \
    .getOrCreate()


# Setup Database and Table Variables
database = "spark_workshop"
table = "books"
tbl = database + "." + table
host = "hdedge-mysql.marketdb.den01.pop"
port = 3306

jdbcDF2 = spark.read \
    .jdbc("jdbc:mysql:" + host + ":" + str(port), tbl,
          properties={"user": "cloudera_su",
                      "password": "somePassword",
                      "jdbcCompliantTruncation": "false",
                      "zeroDateTimeBehavior": "convertToNull"})
jdbcDF2.show()
print("Table " + tbl + " ingested with " + str(jdbcDF2.count()) + " records.")
