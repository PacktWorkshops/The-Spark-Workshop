spark = SparkSession \
   .builder \
   .appName("exercise_sixteen") \
   .getOrCreate()

df.filter(df["salary"] > 10000).filter(df["gender"] == "Female").show()

df.filter(df.salary > 10000).filter(df.gender == "Female").show()

df.filter((df.salary > 150000) & (df.gender == "Male")).count()

df.filter((df["salary"] > 150000) | (df["gender"] == "Female")).count()
