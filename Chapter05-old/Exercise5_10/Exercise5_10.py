spark = SparkSession \
   .builder \
   .appName("exercise_ten") \
   .getOrCreate()

from pyspark.sql.types import StructType, StructField, StringType, LongType

sales_schema = StructType([
  StructField("user_id", LongType(), False),
  StructField("product_item", StringType(), True)
])

sales_field = StructField("total_sales", LongType(), True)

another_schema = sales_schema.add(sales_field)

print(another_schema)
