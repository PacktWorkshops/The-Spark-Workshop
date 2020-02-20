val spark = SparkSession
   .builder()
   .appName("exercise_nineteen")
   .getOrCreate()

import spark.implicits._

val reg_df = df.select(df("registration_dttm"))
reg_df.show(3)
reg_df.printSchema

val long_reg_df = df.select($"registration_dttm".cast("long"))
long_reg_df.show(3)
long_reg_df.printSchema
