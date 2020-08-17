spark = SparkSession \
   .builder \
   .appName("nested_schemas") \
   .getOrCreate()

nested_sales_data = [{"id":101,"name":"Jim","orders":[{"id":1,"price":45.99,"userid":101},{"id":2,"price":17.35,"userid":101}]},{"id":102,"name":"Christina","orders":[{"id":3,"price":245.86,"userid":102}]},{"id":103,"name":"Steve","orders":[{"id":4,"price":7.45,"userid":103},{"id":5,"price":8.63,"userid":103}]}]

from pyspark.sql.types import *

orders_schema = [
  StructField("id", IntegerType(), True),
  StructField("price", DoubleType(), True),
  StructField("userid", IntegerType(), True)
]

sales_schema = StructType([
  StructField("id", IntegerType(), True),
  StructField("name", StringType(), True),
  StructField("orders", ArrayType(StructType(orders_schema)), True)
])

nested_df = spark.createDataFrame(nested_sales_data, sales_schema)

nested_df.show(20, False)
nested_df.printSchema()
