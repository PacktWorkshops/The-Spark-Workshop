spark = SparkSession \
   .builder \
   .appName("some_app_name") \
   .getOrCreate()


# Exercise 2

df = (spark.read
  .format("csv")
  .load("hdfs://…/census_data_2010/stco-mr2010_*.csv"
    , sep=","
    , inferSchema="true"
    , header="true")
)

df.printSchema()

df.show()

df.select("AGEGRP").distinct().count()

df.select(df["IMPRACE"]).distinct().count()

df.count()

# Exercise 3

from pyspark.sql import functions as F

people = (("Alice", 2), ("Jim", 1))

people_df = spark.createDataFrame(people, ["Name", "Sex"])

people_df.withColumn("new_column_name", F.when(people_df["Sex"] == 1, "Male")).show()

from pyspark.sql import functions as F

df.withColumn("sex_mf", F.when(df['SEX'] == 1, "Male").otherwise("Female")) \
  .withColumn("hispanic_origin", F.when(df.ORIGIN == 2, "Hispanic").otherwise("Not Hispanic")) \
  .show(10)


# Exercise 5

from pyspark.sql import functions as F

census_df = df.withColumn("sex_mf", F.when(df['SEX'] == 1, "Male").otherwise("Female")) \
  .withColumn("hispanic_origin", F.when(df.ORIGIN == 2, "Hispanic").otherwise("Not Hispanic")) \
  .withColumn("age_group",
              F.when(df["AGEGRP"] == 1, "Age 0 to 4 years")
              .when(df["AGEGRP"] == 2, "Age 5 to 9 years")
              .when(df["AGEGRP"] == 3, "Age 10 to 14 years")
              .when(df["AGEGRP"] == 4, "Age 15 to 19 years")
              .when(df["AGEGRP"] == 5, "Age 20 to 24 years")
              .when(df["AGEGRP"] == 6, "Age 25 to 29 years")
              .when(df["AGEGRP"] == 7, "Age 30 to 34 years")
              .when(df["AGEGRP"] == 8, "Age 35 to 39 years")
              .when(df["AGEGRP"] == 9, "Age 40 to 44 years")
              .when(df["AGEGRP"] == 10, "Age 45 to 49 years")
              .when(df["AGEGRP"] == 11, "Age 50 to 54 years")
              .when(df["AGEGRP"] == 12, "Age 55 to 59 years")
              .when(df["AGEGRP"] == 13, "Age 60 to 64 years")
              .when(df["AGEGRP"] == 14, "Age 65 to 69 years")
              .when(df["AGEGRP"] == 15, "Age 70 to 74 years")
              .when(df["AGEGRP"] == 16, "Age 75 to 79 years")
              .when(df["AGEGRP"] == 17, "Age 80 to 84 years")
              .when(df["AGEGRP"] == 18, "Age 85 years or older")
              .otherwise("No age given")
             ) \
  .withColumn("race",
              F.when(df["IMPRACE"] == 1, "White alone")
              .when(df["IMPRACE"] == 2, "Black or African American alone")
              .when(df["IMPRACE"] == 3, "American Indian and Alaska Native alone")
              .when(df["IMPRACE"] == 4, "Asian alone")
              .when(df["IMPRACE"] == 5, "Native Hawaiian and Other Pacific Islander alone")
              .when(df["IMPRACE"] == 6, "White and Black or African American")
              .when(df["IMPRACE"] == 7, "White and American Indian and Alaska Native")
              .when(df["IMPRACE"] == 8, "White and Asian")
              .when(df["IMPRACE"] == 9, "White and Native Hawaiian and Other Pacific Islander")
              .when(df["IMPRACE"] == 10, "Black or African American and American Indian and Alaska Native")
              .when(df["IMPRACE"] == 11, "Black or African American and American Indian and Alaska Native")
              .when(df["IMPRACE"] == 12, "Black or African American and Native Hawaiian and Other Pacific Islander")
              .when(df["IMPRACE"] == 13, "American Indian and Alaska Native and Asian")
              .when(df["IMPRACE"] == 14, "American Indian and Alaska Native and Native Hawaiian and Other Pacific Islander")
              .when(df["IMPRACE"] == 15, "Asian and Native Hawaiian and Other Pacific Islander")
              .when(df["IMPRACE"] == 16, "White and Black or African American and American Indian and Alaska Native")
              .when(df["IMPRACE"] == 17, "White and Black or African American and Asian")
              .when(df["IMPRACE"] == 18, "White and Black or African American and Native Hawaiian and Other Pacific Islander")
              .when(df["IMPRACE"] == 19, "White and American Indian and Alaska Native and Asian")
              .when(df["IMPRACE"] == 20, "White and American Indian and Alaska Native and Native Hawaiian and Other Pacific Islander")
              .when(df["IMPRACE"] == 21, "White and Asian and Native Hawaiian and Other Pacific Islander")
              .when(df["IMPRACE"] == 22, "Black or African American and American Indian and Alaska Native and Asian")
              .when(df["IMPRACE"] == 23, "Black or African American and American Indian and Alaska Native and Native Hawaiian and Other Pacific Islander")
              .when(df["IMPRACE"] == 24, "Black or African American and Asian and Native Hawaiian and Other Pacific Islander")
              .when(df["IMPRACE"] == 25, "American Indian and Alaska Native and Asian and Native Hawaiian and Other Pacific Islander")
              .when(df["IMPRACE"] == 26, "White and Black or African American and American Indian and Alaska Native and Asian")
              .when(df["IMPRACE"] == 27, "White and Black or African American and American Indian and Alaska Native and Native Hawaiian and Other Pacific Islander")
              .when(df["IMPRACE"] == 28, "White and Black or African American and Asian and Native Hawaiian and Other Pacific Islander")
              .when(df["IMPRACE"] == 29, "White and American Indian and Alaska Native and Asian and Native Hawaiian and Other Pacific Islander")
              .when(df["IMPRACE"] == 30, "Black or African American and American Indian and Alaska Native and Asian and Native Hawaiian and Other Pacific Islander")
              .when(df["IMPRACE"] == 31, "White and Black or African American and American Indian and Alaska Native and Asian and Native Hawaiian and Other Pacific Islander")
              .otherwise("No race provided")
             ) \
  .withColumnRenamed("STNAME", "us_state") \
  .withColumnRenamed("CTYNAME", "county_name") \
  .withColumnRenamed("RESPOP", "population") \
  .select("us_state", "county_name", "sex_mf", "hispanic_origin", "age_group", "race", "population")


# Exercise 6

from pyspark.sql import functions as F
census_df.agg(F.sum(census_df["population"])).show()

census_df.agg(F.sum(census_df["population"])).collect()

total_population = census_df.agg(F.sum(census_df["population"])).collect()[0][0]

# Exercise 7

from pyspark.sql import functions as F

(census_df.groupBy("us_state").agg(F.sum(census_df["population"]).alias("population"))
  .orderBy(F.col("population").desc())
  .show(10))
