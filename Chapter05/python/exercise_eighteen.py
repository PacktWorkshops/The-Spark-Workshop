spark = SparkSession \
   .builder \
   .appName("exercise_eighteen") \
   .getOrCreate()

(df.select("id", "first_name", "last_name", "gender", "country", "birthdate", "salary")
  .filter(df["country"] == "United States")
  .orderBy(df["gender"].asc(), df["salary"].asc())
  .show())

df.select("id", "first_name", "last_name", "gender", "country", "birthdate", "salary") \
  .filter(df["country"] == "United States") \
  .orderBy(df["gender"].asc(), df["salary"].asc()) \
  .show()
