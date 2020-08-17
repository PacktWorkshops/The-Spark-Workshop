# The Spark Workshop

# Chapter 5
# DataFrames with Spark

# By Craig Covey

# 1
df = spark.read.format("csv").load("hdfs://…/royal_holloway/Schools dispersals data with BHL links.csv"
                                  , sep=","
                                  , inferSchema="true"
                                  , header="true")

# 2
df.printSchema()

# 3
df = df.withColumnRenamed("EventDate", "event_date_string") \
  .withColumnRenamed("Period", "period") \
  .withColumnRenamed("Page number (if applic)", "page_number") \
  .withColumnRenamed("Recipient name (original transcription)", "recipient_name") \
  .withColumnRenamed("Nature of recipient (original transcription)", "formal_school_name") \
  .withColumnRenamed("name", "name") \
  .withColumnRenamed("Institution - common name from Exit Books", "school_name") \
  .withColumnRenamed("Town/City", "city") \
  .withColumnRenamed("Level of study ", "level_of_study") \
  .withColumnRenamed("School type", "school_type") \
  .withColumnRenamed("Denomination/organisation", "denomination_or_org") \
  .withColumnRenamed("No. of objects", "object_count") \
  .withColumnRenamed("Event information (any extra information not in list of specimens)", "event_info") \
  .withColumnRenamed("BHL Exit Book Link", "book_link_1") \
  .withColumnRenamed("BHL School Letter Book Link (Vol 1 only)", "book_link_2")

# 4
df.show(30)

# 5
df = df.drop("book_link_1", "book_link_2")

# 6
from pyspark.sql.types import DateType
from pyspark.sql.functions import to_date

df = df.withColumn("event_date", to_date(df["event_date_string"], "dd.MM.yyyy"))

# 7
kew_df = df.select("event_date", "page_number", "recipient_name", "formal_school_name", "name", "school_name", "city", "level_of_study", "school_type", "denomination_or_org", "object_count")

# 8
kew_df = kew_df.orderBy(df["event_date"].asc())

# 9
kew_df.count()

# 10
kew_df.select(kew_df["city"]).distinct().count()

# 11
from pyspark.sql.functions import col
kew_df.select(kew_df["city"]).distinct().orderBy(col("city").asc()).show(60)

# 12
from pyspark.sql import functions as F

kew_df.groupBy("level_of_study").agg(F.sum(kew_df["object_count"])).orderBy(F.col("sum(object_count)")).show()

# 13
from pyspark.sql import functions as F

kew_df.groupBy(F.year("event_date")).agg(F.avg(df["page_number"])).show(43)

# 14
from pyspark.sql import functions as F

kew_df.groupBy("school_type", "denomination_or_org").agg(F.min(kew_df["object_count"]).alias("min_objects")).show(50)
