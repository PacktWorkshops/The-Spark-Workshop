# The Spark Workshop

# Chapter 5
# DataFrames with Spark

# By Craig Covey

# Exercise 5.23

df.describe().show()

df.describe("last_name", "salary").show()

df.select("salary").summary().show()

df.select("id", "salary").summary("mean", "stddev", "15%", "66%").show()



df.crosstab("country", "gender").show(10)


# Exercise 5.24

df.groupBy().avg().show()

df.groupBy().sum().show()

df.groupBy().max("salary").show()


# Exercise 5.25

df.groupBy("gender").count().show()

df.groupby(df["gender"]).avg().show()

df.groupby(df.gender).min().show()


# Exercise 5.26

df.groupBy("gender").avg("salary").show()

df.groupBy("country").avg("salary", "id").show(7)

# The Aggregate Method

from pyspark.sql.functions import avg
df.groupby("country").agg(avg(df["salary"])).show(5)

from pyspark.sql.functions import sum
df.groupby("country").agg(sum(df.salary)).show(5)


from pyspark.sql.functions import sum
df.groupby("country").agg(sum(df.salary)).show(5)

from pyspark.sql import functions as F
df.groupBy("country").agg(F.max(df.salary), F.avg(df['salary'])).show(5)

from pyspark.sql import functions as purple_dragons
df.groupby("country").agg(purple_dragons.min(df.salary), purple_dragons.max(df.salary)).show(5)


# Other agg() Options

df.groupBy("gender").agg({'salary': 'mean', 'salary': 'sum'}).show()

df.groupBy("gender").agg({"salary": "sum", "id": "max"}).show()


# Exercise 5.28

df.groupby("gender", "country").avg().show(5)

from pyspark.sql import functions as F
df.groupby("title", df["gender"]).agg(F.avg("salary"), F.max("salary")).orderBy(df.title.desc()).show(7)

# Renaming Aggregate Columns

from pyspark.sql import functions as F
df.groupBy("country").agg(F.avg(df.salary).alias("avg_income"), F.max(df['salary']).alias("max_money")).show(7)

from pyspark.sql import functions as F
df.groupBy("country").agg(F.avg(df.salary), F.max(df['salary'])) \
  .withColumnRenamed("max(salary)", "max_money") \
  .withColumnRenamed("avg(salary)", "avg_income") \
  .show(7)
