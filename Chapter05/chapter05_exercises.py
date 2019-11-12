# The Spark Workshop

# Chapter 5
# DataFrames with Spark

# By Craig Covey
# Last modified: 11/12/2019


# 5.3

spark = SparkSession \
   .builder \
   .appName("some_app_name") \
   .getOrCreate()



cats_list = [[1, "tiger"], [2, "lion"], [3, "leopard"]]

df = spark.createDataFrame(cats_list)

df.show()
df.printSchema()


address_data = [["Bob", "1348 Central Park Avenue"], ["Nicole", "734 Southwest 46th Street"], ["Jordan", "3786 Ocean City Drive"]]

address_df = spark.createDataFrame(address_data)

address_df.show()
address_df.show(2, False)



bear_tuple = ((1, "grizzly", "play dead"), (2, "black", "fight back"), (3, "polar", "why are you in the arctic?"), (4, "teddy", "cuddle"))

bear_df = spark.createDataFrame(bear_tuple)

bear_df.show(4, False)
bear_df.printSchema()



dogs_list_dict = [{'id': 1, 'wild_dog': 'wolf'}, {'id': 2, 'wild_dog': 'coyote'}, {'id': 3, 'wild_dog': 'fox'}]

dogs_df2 = spark.createDataFrame(dogs_list_dict)

dogs_df2.show()
dogs_df2.printSchema()


# 5.4

dogs_list = [[1, "wolf"], [2, "coyote"], [3, "fox"]]

dog_df = spark.createDataFrame(dogs_list, ["id", "wild_dog"])

dog_df.show()
dog_df.printSchema()


df = spark.createDataFrame(cats_list)
df.show()
df.schema



from pyspark.sql.types import StructType, StructField

schema = StructType([
  StructField("id", IntegerType(), True),
  StructField("wild_cat", StringType(), True),
  StructField("avg_length", DoubleType(), True)
])



from pyspark.sql.types import *

cats_list = [[1, "tiger", 4.51], [2, "lion", 5.3], [3, "leopard", 3.69], [3, "jaguar", 2.78]]

cat_schema = StructType([
  StructField("id", LongType(), True),
  StructField("cat", StringType(), True),
  StructField("avg_value", DoubleType(), True)
])

cats_df_schema = spark.createDataFrame(cats_list, cat_schema)

cats_df_schema.show()
cats_df_schema.printSchema()



schema1 = StructType([
  StructField("column1", LongType(), True),
  StructField("column2", StringType(), True)
])

schema2 = StructType().add("column1", LongType(), True).add("column2", StringType(), True)

schema3 = StructType().add(StructField("column1", LongType(), True)).add(StructField("column2", StringType(), True))

print(schema1 == schema2)
print(schema1 == schema3)



some_schema = StructType([
  StructField("column1", LongType(), True),
  StructField("column2", StringType(), True)
])

field_3 = StructField("column3", LongType(), True)

another_schema = some_schema.add(field_3)

print(another_schema)




print(cats_df_schema.schema)



df.schema

final_schema = df.schema.add(StructField("column_x", StringType(), True))



print( cats_df_schema.schema.fieldNames() )
print( cat_schema.fieldNames() )



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


# 5.5


parquet_pyspark_df1 = spark.read.parquet("hdfs://user/your_user_name/data/userdata1.parquet")
parquet_pyspark_df2 = spark.read.format("parquet").load("hdfs://user/your_user_name/data/userdata1.parquet")


orc_pyspark_df1 = spark.read.orc("hdfs://user/your_user_name/data/orc/userdata1_orc")

orc_pyspark_df2 = spark.read.format("orc").load("hdfs://user/your_user_name/data/orc/userdata1_orc")


orc_pyspark_df3 = spark.read.orc("hdfs://user/your_user_name/data/orc")

orc_pyspark_df4 = spark.read.format("orc").load("hdfs://user/your_user_name/data/orc/")


json_pyspark_df1 = spark.read.json("hdfs://user/your_user_name/data/books1.json")

json_pyspark_df2 = spark.read.format("json").load("hdfs://user/your_user_name/data/books1.json")



avro_pyspark_df1 = spark.read.format("avro").load("hdfs://user/your_user_name/data/userdata1.avro")
avro_pyspark_df2 = spark.read.format("avro").load("hdfs://user/your_user_name/data/*.avro")
avro_pyspark_df3 = spark.read.foramt("avro").load("hdfs://user/your_user_name/data/userdata1.avro,hdfs://user/your_user_name/data/userdata5.avro,hdfs://user/your_user_name/data/userdata8.avro")



csv_pyspark_df1 = spark.read.csv("hdfs://user/your_user_name/data/csv/userdata1.csv"
  , sep=","
  , inferSchema="true"
  , header="true")

csv_pyspark_df2 = spark.read.format("csv").load("hdfs://user/your_user_name/data/csv/userdata1.csv"
  , sep=","
  , inferSchema="true"
  , header="true")

csv_pyspark_df3 = spark.read.load("hdfs://user/data/your_user_name/csv/userdata1.csv"
  , format="csv"
  , sep=","
  , inferSchema="true"
  , header="true")



# 5.6

df = spark.read.parquet("hdfs://.../sample-data/parquet/*.parquet")



orc_schema = df.schema



df.count()



df.head(2)



df.take(2)



df.select("first_name", "last_name").show(5)



df.select("last_name", "first_name").show(5)



df.select("*").show()



df.select(df['first_name'], df['country'], df['salary']).show()



df.select(df.id, df.first_name, df.last_name).show()



df.select(df['first_name'], df.country, df.salary).show()



df.select("gender").distinct().show()



df.filter(df.salary > 50000).show(4)

df.filter(df["salary"] > 50000).count()



df.filter(df["salary"] > 10000).filter(df["gender"] == "Female").show()

df.filter(df.salary > 10000).filter(df.gender == "Female").show()



df.filter((df.salary > 150000) & (df.gender == "Male")).count()



df.filter((df["salary"] > 150000) | (df["gender"] == "Female")).count()



df.orderBy(df.birthdate.desc()).show()
df.orderBy(df["first_name"].asc()).show()



df.orderBy(df.last_name.desc(), df.country.asc()).show()
df.orderBy(df["title"].asc(), df["last_name"].desc()).show()

# 5.7


df.schema



df.columns[2]
df.columns[2:5]



df.schema.names



df.dtypes



df.select(df.id).take(5)



string_id_df = df.select(df.id.cast("string"))
string_id_df.take(5)



string_id_df.schema



df.select(df.id.cast("string").alias('new_id')).show(5)



df.drop("ip_address")
df.drop("comments", "registration_dttm")



df.drop(df.first_name)
df.drop(df.first_name).drop(df.title)



print(df.columns)
df_renamed = df.withColumnRenamed("title", "role")
print(df_renamed.columns)



df.withColumn("append_id", df["id"] + 10)



df.withColumn("append_id", df["id"] + 10).select("id", "append_id").show(5)



from pyspark.sql.functions import lit
server_df = df.withColumn("constant_string", lit("generated by server")).select("id", "constant_string")
server_df.show(5)
server_df.printSchema()



from pyspark.sql.functions import col, lower, upper

df.withColumn("FIRST_NAME_UPPER", upper(col("first_name"))).select("id", "first_name", "FIRST_NAME_UPPER").show(3)



from pyspark.sql.types import DateType
registration_df = df.withColumn("registration_date", df.registration_dttm.cast(DateType())).select("id", "registration_dttm", "registration_date")
registration_df.show(3)
registration_df.printSchema()



# 5.8



df = spark.read.parquet("hdfs://.../sample-data/parquet/*.parquet")



df.describe().show()



df.describe("last_name", "salary").show()



df.select("salary").summary().show()



df.select("id", "salary").summary("mean", "stddev", "15%", "66%").show()



df.crosstab("country", "gender").show(10)



df.groupBy().avg().show()



df.groupBy().sum().show()



df.groupBy().max("salary").show()



df.groupBy("gender").count().show()



df.groupby(df["gender"]).avg().show()



df.groupby(df.gender).min().show()



df.groupBy("gender").avg("salary").show()



df.groupBy("country").avg("salary", "id").show(7)



from pyspark.sql.functions import avg
df.groupby("country").agg(avg(df["salary"])).show(5)



from pyspark.sql.functions import sum
df.groupby("country").agg(sum(df.salary)).show(5)



from pyspark.sql.functions import sum
df.groupby("country").agg(sum(df.salary)).show(5)



from pyspark.sql import functions as F
df.groupBy("country").agg(F.max(df.salary), F.avg(df['salary'])).show(5)

from pyspark.sql import functions as purple_dragons
df.groupby("country").agg(purple_dragons.min(df.salary), purple_dragons.max(df.salary)).show(5)



df.groupBy("gender").agg({'salary': 'mean', 'salary': 'sum'}).show()



df.groupBy("gender").agg({"salary": "sum", "id": "max"}).show()



df.groupby("gender", "country").avg().show(5)



from pyspark.sql import functions as F
df.groupby("title", df["gender"]).agg(F.avg("salary"), F.max("salary")).orderBy(df.title.desc()).show(7)



from pyspark.sql import functions as F
df.groupBy("country").agg(F.avg(df.salary).alias("avg_income"), F.max(df['salary']).alias("max_money")).show(7)


from pyspark.sql import functions as F
df.groupBy("country").agg(F.avg(df.salary), F.max(df['salary'])) \
  .withColumnRenamed("max(salary)", "max_salary") \
  .withColumnRenamed("avg(salary)", "avg_salary") \
  .show(7)
