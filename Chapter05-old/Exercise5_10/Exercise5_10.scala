val spark = SparkSession
   .builder()
   .appName("exercise_ten")
   .getOrCreate()

import org.apache.spark.sql.types.{StructType, StructField, StringType, LongType}

val sales_schema = StructType(List(
 StructField("user_id", LongType, true),
 StructField("product_item", StringType, true)
))

val sales_field = StructField("total_sales", LongType, true)

val another_schema = sales_schema.add(sales_field)

println(another_schema)
