// The Spark Workshop

// Chapter 5
// DataFrames with Spark

// By Craig Covey
// Last modified: 11/12/2019

// 5.3


val spark = SparkSession
   .builder()
   .appName("some_app_name")
   .getOrCreate()



val reptile_species_state = List(("Arizona", 97), ("Florida", 103), ("Texas", 139))

val reptile_df = spark.createDataFrame(reptile_species_state)

reptile_df.show()
reptile_df.printSchema



val bird_species_state = Seq(("Alaska", 506), ("California", 683), ("Colorado", 496))

val birds_df = spark.createDataFrame(bird_species_state).toDF("state","bird_species")

birds_df.show()
birds_df.printSchema



// 5.4


import org.apache.spark.sql.types.{StructType, StructField}

val schema = StructType(
  List(
    StructField("id", IntegerType, true),
    StructField("wild_cat", StringType, true),
    StructField("avg_length", DoubleType, true)
  )
)



import org.apache.spark.sql.types._

val fruit = Seq(
  Row(1, "banana"),
  Row(2, "strawberry"),
  Row(3, "apple"),
  Row(4, "kiwi")
)

val fruit_schema = StructType(
  List(
    StructField("ID", IntegerType, true),
    StructField("Fruit", StringType, true)
  )
)

val fruit_df = spark.createDataFrame(spark.sparkContext.parallelize(fruit), fruit_schema)

fruit_df.show()
fruit_df.printSchema()



val schema1 = StructType(List(
  StructField("column1", LongType, true),
  StructField("column2", StringType, true)
))

val schema2 = StructType(List()).add("column1", LongType, true).add("column2", StringType, true)

val schema3 = StructType(List()).add(StructField("column1", LongType, true)).add(StructField("column2", StringType, true))

println(schema1 == schema2)
println(schema1 == schema3)



val some_schema = StructType(List(
  StructField("column1", LongType, true),
  StructField("column2", StringType, true)
))

val field_3 = StructField("column3", LongType, true)

val another_schema = some_schema.add(field_3)

println(another_schema)



println(fruit_df.schema)



val columns1 = fruit_df.schema.fieldNames
val columns2 = fruit_schema.fieldNames



// 5.5


val parquet_scala_df1 = spark.read.parquet("hdfs://user/your_user_name/data/userdata1.parquet")
val parquet_scala_df2 = spark.read.format("parquet").load("hdfs://user/your_user_name/data/userdata1.parquet")



val orc_scala_df1 = spark.read.orc("hdfs://user/your_user_name/data/orc/userdata1_orc")

val orc_scala_df2 = spark.read.format("orc").load("hdfs://user/your_user_name/data/orc/userdata1_orc")



val orc_scala_df3 = spark.read.orc("hdfs://user/your_user_name/data/orc")

val orc_scala_df4 = spark.read.format("orc").load("hdfs://user/your_user_name/data/orc/")



val json_scala_df1 = spark.read.json("hdfs://user/your_user_name/data/books1.json")

val json_scala_df2 = spark.read.format("json").load("hdfs://user/your_user_name/data/books1.json")



val avro_scala_df1 = spark.read.format("avro").load("hdfs://user/your_user_name/data/userdata1.avro")



val avro_scala_df2 = spark.read.format("avro").load("hdfs://user/your_user_name/data/*.avro")



val avro_scala_df3 = spark.read.foramt("avro").load("hdfs://user/your_user_name/data/userdata1.avro,hdfs://user/your_user_name/data/userdata5.avro,hdfs://user/your_user_name/data/userdata8.avro")



val csv_scala_df1 = spark.read
  .option("sep", ",")
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("hdfs://user/your_user_name/data/csv/userdata1.csv")

val csv_scala_df2 = spark.read
  .format("csv")
  .option("sep", ",")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("hdfs://user/your_user_name/data/csv/userdata1.csv")


// 5.6


val df = spark.read.parquet("hdfs://.../sample-data/parquet/*.parquet")



df.head(2)



df.take(2)



df.select("first_name", "last_name").show(5)



df.select("last_name", "first_name").show(5)



df.select("*").show()



df.select(df("first_name"), df("country"), df("salary").show()



import spark.implicits._
df.select($"id", $"gender", $"birthdate").show()


df.select(df("first_name"), $"country", $"salary").show()



df.select("country").distinct.show()



df.filter($"first_name" === "Albert").count()



df.filter($"salary" > 10000).filter($"gender" === "Female").show()



df.filter($"salary" > 150000 && $"country" === "United States").count()



df.filter($"salary" > 150000 || $"country" === "United States").count()



df.orderBy($"birthdate".desc).show(5)



df.orderBy(df("birthdate").desc, df("country".desc)).show(5)
df.orderBy($"birthdate".desc, $"country".desc).show(5)


df.select("id", "first_name", "last_name", "gender", "country", "birthdate", "salary")
  .filter(df("country") === "United States")
  .orderBy(df("gender").asc, df("salary").desc)
  .show()



// 5.7


val df = spark.read.format("avro").load("hdfs://.../sample-data/avro/*.avro")



val df = spark.read.parquet("wasbs://test@devcloud2eastus.blob.core.windows.net/sample-data/parquet/*.parquet")



df.columns



df.columns(1)
df.columns.slice(1, 4)



df.schema.names



df.dtypes



val reg_df = df.select(df("registration_dttm"))
reg_df.show(3)
reg_df.printSchema



val long_reg_df = df.select($"registration_dttm".cast("long"))
long_reg_df.show(3)
long_reg_df.printSchema



df.select($"registration_dttm".cast("long").alias("seconds_since_1970")).show(5)



df.drop("ip_address")
df.drop("comments", "registration_dttm")



df.drop($"salary")
df.drop($"comments").drop($"salary")



println(df.columns.mkString(" "))
val df_renamed = df.withColumnRenamed("birthdate", "Date_of_Birth")
println(df_renamed.columns.mkString(" "))



df.withColumn("monthly_salary", $"salary" / 12).select("first_name", "salary", "monthly_salary").show(5)



import org.apache.spark.sql.functions.round

df.withColumn("monthly_salary", round($"salary" / 12, 2))
  .select("first_name", "salary", "monthly_salary")
  .show(5)



df.withColumn("constant_value", 4).show(5)



import org.apache.spark.sql.functions.lit
df.withColumn("constant_value", lit(4)).show(5)



import spark.implicits._
import org.apache.spark.sql.functions.{col, lower, upper}

df.withColumn("last_name_lower", lower($"last_name")).select("id", "last_name", "last_name_lower").show(3)



import spark.implicits._
import org.apache.spark.sql.types.DateType
val new_df = df.withColumn("registration_dttm", $"registration_dttm".cast(DateType))
new_df.select("registration_dttm", "first_name", "last_name").show(3)
new_df.printSchema



import spark.implicits._
df.withColumn("monthly_salary", $"salary" / 12).select(df("first_name"), df("salary"), df("monthly_salary")).show(5)



import spark.implicits._
df.withColumn("monthly_salary", $"salary" / 12).select("first_name", "salary", "monthly_salary").show(5)


// 5.8



val df = spark.read.parquet("hdfs://.../sample-data/parquet/*.parquet")



df.describe().show()



df.describe("last_name", "salary").show()



df.select("salary").summary().show()



df.select("id", "salary").summary("mean", "stddev", "15%", "66%").show()



df.stat.crosstab("country", "gender").show(10)



df.groupBy().avg().show()



df.groupBy().sum().show()



df.groupBy().max("salary").show()



df.groupBy("gender").count().show()



import spark.implicits._
df.groupBy($"gender").max().show()



df.groupBy($"gender").avg("salary").show()



df.groupBy("country").avg("salary", "id").show(7)



import spark.implicits._
import org.apache.spark.sql.functions.max
df.groupBy("country").agg(max($"salary")).show(5)



import org.apache.spark.sql.functions.min
df.groupBy("country").agg(min(df("salary"))).show(5)



import spark.implicits._
import org.apache.spark.sql.functions.max
df.groupBy("country").agg(max($"salary")).show(5)



import spark.implicits._
import org.apache.spark.sql.functions.{avg, min, max, sum}
df.groupBy("gender").agg(avg(df("salary")), min(df("salary")), max($"salary"), sum($"salary")).show()



df.groupBy("gender").agg(Map("salary" -> "max", "salary" -> "avg")).show()



df.groupBy("gender").agg(Map("salary" -> "max", "id" -> "avg")).show()



df.groupby("gender", "country").avg().show(5)



import spark.implicits._
import org.apache.spark.sql.functions.{max, min}
df.groupBy("country", "gender").agg(max(df("salary")), min($"salary"))
  .orderBy($"country".desc)
  .show(7)



import org.apache.spark.sql.functions.{avg, max}
df.groupBy("country").agg(avg(df("salary")).alias("avg_income"), max(df("salary")).alias("max_money")).show(7)



from pyspark.sql import functions as F
df.groupBy("country").agg(F.avg(df.salary), F.max(df['salary'])) \
  .withColumnRenamed("max(salary)", "max_salary") \
  .withColumnRenamed("avg(salary)", "avg_salary") \
  .show(7)
