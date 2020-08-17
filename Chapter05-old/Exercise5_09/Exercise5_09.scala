val spark = SparkSession
   .builder()
   .appName("exercise_nine")
   .getOrCreate()

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
