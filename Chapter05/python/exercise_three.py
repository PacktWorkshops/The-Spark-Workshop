spark = SparkSession \
   .builder \
   .appName("exercise_three") \
   .getOrCreate()

programming_languages = ((1, "Java", "Scalable"), (2, "C", "Portable"), (3, "Python", "Big Data, ML, AI, Robotics"), (4, "JavaScript", "Web Browsers"), (5, "Ruby", "Web Apps"))

prog_lang_df = spark.createDataFrame(programming_languages)

prog_lang_df.show(5, False)
prog_lang_df.printSchema()
