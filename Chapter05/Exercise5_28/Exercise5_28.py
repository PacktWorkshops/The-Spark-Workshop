spark = SparkSession \
   .builder \
   .appName("exercise_twentyeight") \
   .getOrCreate()

df.groupby("gender", "country").avg().show(5)

from pyspark.sql import functions as F
df.groupby("title", df["gender"]).agg(F.avg("salary"), F.max("salary")).orderBy(df.title.desc()).show(7)
