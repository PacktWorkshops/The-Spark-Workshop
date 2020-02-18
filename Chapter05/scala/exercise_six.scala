val spark = SparkSession
   .builder()
   .appName("exercise_six")
   .getOrCreate()

val bird_species_state = Seq(("Alaska", 506), ("California", 683), ("Colorado", 496))

val birds_df = spark.createDataFrame(bird_species_state).toDF("state","bird_species")

birds_df.show()
birds_df.printSchema
