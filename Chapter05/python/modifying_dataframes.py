# The Spark Workshop

# Chapter 5
# DataFrames with Spark

# By Craig Covey
spark = SparkSession \
   .builder \
   .appName("modifying_dataframes") \
   .getOrCreate()


# Exercise 5.13

df.head(2)
df.take(2)

# Selecting Columns of a DataFrame

df.select("first_name", "last_name").show(5)

df.select("last_name", "first_name").show(5)

df.select("*").show()

df.select(df['first_name'], df['country'], df['salary']).show()

df.select(df.id, df.first_name, df.last_name).show()

df.select(df['first_name'], df.country, df.salary).show()

# Exercise 5.14

df.select("gender").distinct().show()

df.select("country").distinct.show(5)

# Exercise 5.15

df.filter(df.salary > 50000).show(4)

df.filter(df["salary"] > 50000).count()

# Exercise 5.16

df.filter(df["salary"] > 10000).filter(df["gender"] == "Female").show()

df.filter(df.salary > 10000).filter(df.gender == "Female").show()

df.filter((df.salary > 150000) & (df.gender == "Male")).count()
df.filter((df["salary"] > 150000) | (df["gender"] == "Female")).count()

# Exercise 5.17

df.orderBy(df.birthdate.desc()).show()
df.orderBy(df["first_name"].asc()).show()

df.orderBy(df.last_name.desc(), df.country.asc()).show()
df.orderBy(df["title"].asc(), df["last_name"].desc()).show()

# Exercise 5.18

(df.select("id", "first_name", "last_name", "gender", "country", "birthdate", "salary")
  .filter(df["country"] == "United States")
  .orderBy(df["gender"].asc(), df["salary"].asc())
  .show())

df.select("id", "first_name", "last_name", "gender", "country", "birthdate", "salary") \
  .filter(df["country"] == "United States") \
  .orderBy(df["gender"].asc(), df["salary"].asc()) \
  .show()
