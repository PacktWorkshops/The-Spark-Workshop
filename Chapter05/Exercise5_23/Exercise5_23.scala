val spark = SparkSession
   .builder()
   .appName("exercise_twentythree")
   .getOrCreate()


df.describe().show()

df.describe("last_name", "salary").show()

df.select("salary").summary().show()

df.select("id", "salary").summary("mean", "stddev", "15%", "66%").show()
