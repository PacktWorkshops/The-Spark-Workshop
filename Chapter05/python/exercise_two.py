spark = SparkSession \
   .builder \
   .appName("exercise_two") \
   .getOrCreate()

address_data = [["Bob", "1348 Central Park Avenue"], ["Nicole", "734 Southwest 46th Street"], ["Jordan", "3786 Ocean City Drive"]]

address_df = spark.createDataFrame(address_data)

address_df.show()

address_df.show(2, False)
