# The Spark Workshop

# Chapter 5
# DataFrames with Spark

# By Craig Covey


spark = SparkSession \
   .builder \
   .appName("some_app_name") \
   .getOrCreate()


# Exercise 5.01
hadoop_list = [[1, "MapReduce"], [2, "YARN"], [3, "Hive"], [4, "Pig"], [5, "Spark"], [6, "Zookeeper"]]

hadoop_df = spark.createDataFrame(hadoop_list)

hadoop_df.show()
hadoop_df.printSchema()


# Exercise 5.02
address_data = [["Bob", "1348 Central Park Avenue"], ["Nicole", "734 Southwest 46th Street"], ["Jordan", "3786 Ocean City Drive"]]

address_df = spark.createDataFrame(address_data)

address_df.show()
address_df.show(2, False)


# Exercise 5.03
programming_languages = ((1, "Java", "Scalable"), (2, "C", "Portable"), (3, "Python", "Big Data, ML, AI, Robotics"), (4, "JavaScript", "Web Browsers"), (5, "Ruby", "Web Apps"))

prog_lang_df = spark.createDataFrame(programming_languages)

prog_lang_df.show(5, False)
prog_lang_df.printSchema()


# Exercise 5.04
top_mobile_phones = [{"Manufacturer": "Nokia", "Model": "1100", "Year": 2003, "Million_Units": 250}, {"Manufacturer": "Nokia", "Model": "1110", "Year": 2005, "Million_Units": 250}, {"Manufacturer": "Apple", "Model": "iPhone 6 & 6+", "Year": 2014, "Million_Units": 222}]

mobile_phones_df = spark.createDataFrame(top_mobile_phones)

mobile_phones_df.show()
mobile_phones_df.printSchema()



# DataFrame Schemas

df1 = spark.createDataFrame(computer_sales)
df2 = spark.createDataFrame(computer_sales, None)
df3 = spark.createDataFrame(computer_sales, ["product_code", "computer_name", "sales"])
df4 = spark.createDataFrame(computer_sales, ["product_code", "computer_name", "sales"], 2)
df5 = spark.createDataFrame(computer_sales, ["product_code", "computer_name", "sales"], len(computer_sales))


# Exercise 5.07
home_computers = [["Honeywell", "Honeywell 316#Kitchen Computer", "DDP 16 Minicomputer", 1969], ["Apple Computer", "Apple II series", "6502", 1977], ["Bally Consumer Products", "Bally Astrocade", "Z80", 1977]]

computers_df = spark.createDataFrame(home_computers, ["Manufacturer", "Model", "Processor", "Year"])

computers_df.show()
computers_df.printSchema()


# Schemas, StructTypes, and StructFields

computers_df.schema

from pyspark.sql.types import StructType, StructField

schema = StructType([
  StructField("Manufacturer", StringType(), True),
  StructField("Model", StringType(), True),
  StructField("Processor", StringType(), True),
  StructField("Year", LongType(), True)
])

from pyspark.sql.types import *

from pyspark.sql.types import StructType, StructField

# Exercise 5.08
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


# The add() Method
from pyspark.sql.types import StructType, StructField, StringType, LongType

schema1 = StructType([
  StructField("id_column", LongType(), True),
  StructField("product_desc", StringType(), True)
])

schema2 = StructType().add("id_column", LongType(), True).add("product_desc", StringType(), True)

schema3 = StructType().add(StructField("id_column", LongType(), True)).add(StructField("product_desc", StringType(), True))

print(schema1 == schema2)
print(schema1 == schema3)


# Exercise 5.10
from pyspark.sql.types import StructType, StructField, StringType, LongType

sales_schema = StructType([
  StructField("user_id", LongType(), False),
  StructField("product_item", StringType(), True)
])

sales_field = StructField("total_sales", LongType(), True)

another_schema = sales_schema.add(sales_field)

print(another_schema)


# Use the add() method when adding columns to a DataFrame

print(customer_df.schema)

final_schema = customer_df.schema.add(StructField("new_column", StringType(), True))

print(final_schema)

print( customer_df.schema.fieldNames() )
print( customer_schema.fieldNames() )


# Nested Schemas

nested_sales_data = [{"id":101,"name":"Jim","orders":[{"id":1,"price":45.99,"userid":101},{"id":2,"price":17.35,"userid":101}]},{"id":102,"name":"Christina","orders":[{"id":3,"price":245.86,"userid":102}]},{"id":103,"name":"Steve","orders":[{"id":4,"price":7.45,"userid":103},{"id":5,"price":8.63,"userid":103}]}]

ugly_df = spark.createDataFrame(nested_sales_data)
ugly_df.show(20, False)
ugly_df.printSchema()

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
