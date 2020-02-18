spark = SparkSession \
   .builder \
   .appName("exercise_eight") \
   .getOrCreate()

from pyspark.sql.types import *
customer_list = [[111, "Jim", 45.51], [112, "Fred", 87.3], [113, "Jennifer", 313.69], [114, "Lauren", 28.78]]

customer_schema = StructType([
  StructField("customer_id", LongType(), True),
  StructField("first_name", StringType(), True),
  StructField("avg_shopping_cart", DoubleType(), True)
])

customer_df = spark.createDataFrame(customer_list, customer_schema)

customer_df.show()
customer_df.printSchema()
