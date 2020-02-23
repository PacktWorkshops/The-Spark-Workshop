spark = SparkSession \
   .builder \
   .appName("dataframe_aggregations") \
   .getOrCreate()


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

# Other agg() Options

df.groupBy("gender").agg({'salary': 'mean', 'salary': 'sum'}).show()

df.groupBy("gender").agg({"salary": "sum", "id": "max"}).show()

# Renaming Aggregate Columns

from pyspark.sql import functions as F
df.groupBy("country").agg(F.avg(df.salary).alias("avg_income"), F.max(df['salary']).alias("max_money")).show(7)

from pyspark.sql import functions as F
df.groupBy("country").agg(F.avg(df.salary), F.max(df['salary'])) \
  .withColumnRenamed("max(salary)", "max_money") \
  .withColumnRenamed("avg(salary)", "avg_income") \
  .show(7)
