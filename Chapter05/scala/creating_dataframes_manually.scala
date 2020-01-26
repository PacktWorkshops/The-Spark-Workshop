// The Spark Workshop

// Chapter 5
// DataFrames with Spark

// By Craig Covey


// Exercise 5.05
val reptile_species_state = List(("Arizona", 97), ("Florida", 103), ("Texas", 139))

val reptile_df = spark.createDataFrame(reptile_species_state)

reptile_df.show()
reptile_df.printSchema


// Exercise 5.06
val bird_species_state = Seq(("Alaska", 506), ("California", 683), ("Colorado", 496))

val birds_df = spark.createDataFrame(bird_species_state).toDF("state","bird_species")

birds_df.show()
birds_df.printSchema



// Schemas, StructTypes, and StructFields

computers_df.schema

import org.apache.spark.sql.types.{StructType, StructField}

val schema = StructType(
  List(
    StructField("Manufacturer", IntegerType, true),
    StructField("Model", StringType, true),
    StructField("Processor", StringType, true),
    StructField("Year", LongType, true)
  )
)

import org.apache.spark.sql.types._

import org.apache.spark.sql.types.{StructType, StructField}


// Exercise 5.09
import org.apache.spark.sql.types._

val grocery_items = Seq(
  Row(1, "stuffing", 4.67),
  Row(2, "milk", 3.69),
  Row(3, "rols", 2.99),
  Row(4, "potatoes", 5.15),
  Row(5, "turkey", 23.99)
)

val grocery_schema = StructType(
  List(
    StructField("id", IntegerType, true),
    StructField("item", StringType, true),
    StructField("price", DoubleType, true)
  )
)

val grocery_df = spark.createDataFrame(spark.sparkContext.parallelize(grocery_items), grocery_schema)

grocery_df.show()
grocery_df.printSchema()


// The add() Method
import org.apache.spark.sql.types.{StructType, StructField, StringType, LongType}

val scala_schema1 = StructType(List(
  StructField("id_column", LongType, true),
  StructField("product_desc", StringType, true)
))

val scala_schema2 = StructType(List()).add("id_column", LongType, true).add("product_desc", StringType, true)

val scala_schema3 = StructType(List()).add(StructField("id_column", LongType, true)).add(StructField("product_desc", StringType, true))

println(scala_schema1 == scala_schema2)
println(scala_schema1 == scala_schema3)


// Exercise 5.10
import org.apache.spark.sql.types.{StructType, StructField, StringType, LongType}

val sales_schema = StructType(List(
  StructField("user_id", LongType, true),
  StructField("product_item", StringType, true)
))

val sales_field = StructField("total_sales", LongType, true)

val another_schema = sales_schema.add(sales_field)

println(another_schema)


// Use the add() method when adding columns to a DataFrame

println(grocery_df.schema)

val final_schema = grocery_df.schema.add(StructField("new_column", StringType, true))

println(final_schema)

val columns1 = grocery_df.schema.fieldNames
val columns2 = grocery_schema.fieldNames
