val spark = SparkSession
   .builder()
   .appName("exercise_five")
   .getOrCreate()

val reptile_species_state = List(("Arizona", 97), ("Florida", 103), ("Texas", 139))

val reptile_df = spark.createDataFrame(reptile_species_state)

reptile_df.show()
reptile_df.printSchema
