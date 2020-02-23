val spark = SparkSession
   .builder()
   .appName("exercise_twentyfour")
   .getOrCreate()

df.groupBy().avg().show()

df.groupBy().sum().show()

df.groupBy().max("salary").show()
