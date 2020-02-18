spark = SparkSession \
   .builder \
   .appName("exercise_four") \
   .getOrCreate()

top_mobile_phones = [{"Manufacturer": "Nokia", "Model": "1100", "Year": 2003, "Million_Units": 250}, {"Manufacturer": "Nokia", "Model": "1110", "Year": 2005, "Million_Units": 250}, {"Manufacturer": "Apple", "Model": "iPhone 6 & 6+", "Year": 2014, "Million_Units": 222}]

mobile_phones_df = spark.createDataFrame(top_mobile_phones)

mobile_phones_df.show()
mobile_phones_df.printSchema()
