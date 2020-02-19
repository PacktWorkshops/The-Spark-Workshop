spark = SparkSession \
   .builder \
   .appName("exercise_seventeen") \
   .getOrCreate()

df.orderBy(df.birthdate.desc()).show()

df.orderBy(df["first_name"].asc()).show()

df.orderBy(df.last_name.desc(), df.country.asc()).show()

df.orderBy(df["title"].asc(), df["last_name"].desc()).show()
