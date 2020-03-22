spark = SparkSession \
   .builder \
   .appName("exercise_twentyone") \
   .getOrCreate()

from pyspark.sql.functions import col, lower, upper

df.withColumn("FIRST_NAME_UPPER", upper(col("first_name"))).select("id", "first_name", "FIRST_NAME_UPPER").show(3)
