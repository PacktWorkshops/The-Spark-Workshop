val spark = SparkSession
   .builder()
   .appName("some_app_name")
   .getOrCreate()


// Exericse 2

val df = spark.read
  .option("sep", ",")
  .option("inferSchema", "true")
  .option("header", "true")
  .csv("hdfs://…/census_data_2010/stco-mr2010_*.csv")


df.printSchema()

df.show()

df.select("AGEGRP").distinct().count()

import spark.implicits._
df.select($"IMPRACE").distinct.count()

df.count()


// Exercise 3

import sqlContext.implicits._ // for `toDF` and $""
import org.apache.spark.sql.functions._ // for `when`

df.withColumn("sex_mf", when($"SEX" === 1, "Male").otherwise("Female")).show(10)


// Exercise 4

import org.apache.spark.sql.functions._ // for `when`

df.withColumn("sex_mf", when($"SEX" === 1, "Male").otherwise("Female"))
  .withColumn("hispanic_origin", when(df("SEX") === 2, "Hispanic").otherwise("Not Hispanic"))
  .withColumn("age_group",
              when(df("AGEGRP") === 1, "Age 0 to 4 years")
              .when(df("AGEGRP") === 2, "Age 5 to 9 years")
              .when(df("AGEGRP") === 3, "Age 10 to 14 years")
              .when(df("AGEGRP") === 4, "Age 15 to 19 years")
              .when(df("AGEGRP") === 5, "Age 20 to 24 years")
              .when(df("AGEGRP") === 6, "Age 25 to 29 years")
              .when(df("AGEGRP") === 7, "Age 30 to 34 years")
              .when(df("AGEGRP") === 8, "Age 35 to 39 years")
              .when(df("AGEGRP") === 9, "Age 40 to 44 years")
              .when(df("AGEGRP") === 10, "Age 45 to 49 years")
              .when(df("AGEGRP") === 11, "Age 50 to 54 years")
              .when(df("AGEGRP") === 12, "Age 55 to 59 years")
              .when(df("AGEGRP") === 13, "Age 60 to 64 years")
              .when(df("AGEGRP") === 14, "Age 65 to 69 years")
              .when(df("AGEGRP") === 15, "Age 70 to 74 years")
              .when(df("AGEGRP") === 16, "Age 75 to 79 years")
              .when(df("AGEGRP") === 17, "Age 80 to 84 years")
              .when(df("AGEGRP") === 18, "Age 85 years or older")
              .otherwise("No age given")
             )
  .show(10)


// Exercise 5

import sqlContext.implicits._ // for `toDF` and $""
import org.apache.spark.sql.functions._ // for `when`

val census_df = df
  .withColumn("sex_mf", when($"SEX" === 1, "Male").otherwise("Female"))
  .withColumn("hispanic_origin", when(df("SEX") === 2, "Hispanic").otherwise("Not Hispanic"))
  .withColumn("age_group",
              when(df("AGEGRP") === 1, "Age 0 to 4 years")
              .when(df("AGEGRP") === 2, "Age 5 to 9 years")
              .when(df("AGEGRP") === 3, "Age 10 to 14 years")
              .when(df("AGEGRP") === 4, "Age 15 to 19 years")
              .when(df("AGEGRP") === 5, "Age 20 to 24 years")
              .when(df("AGEGRP") === 6, "Age 25 to 29 years")
              .when(df("AGEGRP") === 7, "Age 30 to 34 years")
              .when(df("AGEGRP") === 8, "Age 35 to 39 years")
              .when(df("AGEGRP") === 9, "Age 40 to 44 years")
              .when(df("AGEGRP") === 10, "Age 45 to 49 years")
              .when(df("AGEGRP") === 11, "Age 50 to 54 years")
              .when(df("AGEGRP") === 12, "Age 55 to 59 years")
              .when(df("AGEGRP") === 13, "Age 60 to 64 years")
              .when(df("AGEGRP") === 14, "Age 65 to 69 years")
              .when(df("AGEGRP") === 15, "Age 70 to 74 years")
              .when(df("AGEGRP") === 16, "Age 75 to 79 years")
              .when(df("AGEGRP") === 17, "Age 80 to 84 years")
              .when(df("AGEGRP") === 18, "Age 85 years or older")
              .otherwise("No age given")
             )
  .withColumn("race",
              when($"IMPRACE" === 1, "White alone")
              .when($"IMPRACE" === 2, "Black or African American alone")
              .when($"IMPRACE" === 3, "American Indian and Alaska Native alone")
              .when($"IMPRACE" === 4, "Asian alone")
              .when($"IMPRACE" === 5, "Native Hawaiian and Other Pacific Islander alone")
              .when($"IMPRACE" === 6, "White and Black or African American")
              .when($"IMPRACE" === 7, "White and American Indian and Alaska Native")
              .when($"IMPRACE" === 8, "White and Asian")
              .when($"IMPRACE" === 9, "White and Native Hawaiian and Other Pacific Islander")
              .when($"IMPRACE" === 10, "Black or African American and American Indian and Alaska Native")
              .when($"IMPRACE" === 11, "Black or African American and American Indian and Alaska Native")
              .when($"IMPRACE" === 12, "Black or African American and Native Hawaiian and Other Pacific Islander")
              .when($"IMPRACE" === 13, "American Indian and Alaska Native and Asian")
              .when($"IMPRACE" === 14, "American Indian and Alaska Native and Native Hawaiian and Other Pacific Islander")
              .when($"IMPRACE" === 15, "Asian and Native Hawaiian and Other Pacific Islander")
              .when($"IMPRACE" === 16, "White and Black or African American and American Indian and Alaska Native")
              .when($"IMPRACE" === 17, "White and Black or African American and Asian")
              .when($"IMPRACE" === 18, "White and Black or African American and Native Hawaiian and Other Pacific Islander")
              .when($"IMPRACE" === 19, "White and American Indian and Alaska Native and Asian")
              .when($"IMPRACE" === 20, "White and American Indian and Alaska Native and Native Hawaiian and Other Pacific Islander")
              .when($"IMPRACE" === 21, "White and Asian and Native Hawaiian and Other Pacific Islander")
              .when($"IMPRACE" === 22, "Black or African American and American Indian and Alaska Native and Asian")
              .when($"IMPRACE" === 23, "Black or African American and American Indian and Alaska Native and Native Hawaiian and Other Pacific Islander")
              .when($"IMPRACE" === 24, "Black or African American and Asian and Native Hawaiian and Other Pacific Islander")
              .when($"IMPRACE" === 25, "American Indian and Alaska Native and Asian and Native Hawaiian and Other Pacific Islander")
              .when($"IMPRACE" === 26, "White and Black or African American and American Indian and Alaska Native and Asian")
              .when($"IMPRACE" === 27, "White and Black or African American and American Indian and Alaska Native and Native Hawaiian and Other Pacific Islander")
              .when($"IMPRACE" === 28, "White and Black or African American and Asian and Native Hawaiian and Other Pacific Islander")
              .when($"IMPRACE" === 29, "White and American Indian and Alaska Native and Asian and Native Hawaiian and Other Pacific Islander")
              .when($"IMPRACE" === 30, "Black or African American and American Indian and Alaska Native and Asian and Native Hawaiian and Other Pacific Islander")
              .when($"IMPRACE" === 31, "White and Black or African American and American Indian and Alaska Native and Asian and Native Hawaiian and Other Pacific Islander")
              .otherwise("No race provided")
             )
  .withColumnRenamed("STNAME", "us_state")
  .withColumnRenamed("CTYNAME", "county_name")
  .withColumnRenamed("RESPOP", "population")
  .select("us_state", "county_name", "sex_mf", "hispanic_origin", "age_group", "race", "population")


census_df.printSchema()

census_df.show(10, 40)


// Exercise 6

import org.apache.spark.sql.functions.sum
census_df.agg(sum(census_df ("population"))).show()

census_df.agg(sum(census_df("population"))).collect()

val total_population = census_df.agg(sum(census_df("population"))).collect()(0)(0)


// Exercise 7

import spark.implicits._
import org.apache.spark.sql.functions.col

census_df
  .filter($"us_state" === "Ohio")
  .filter($"age_group" === "Age 30 to 34 years" || $"age_group" === "Age 35 to 39 years")
  .filter($"race" === "Black or African American alone")
  .groupBy("county_name")
  .agg(sum($"population").alias("population"))
  .orderBy(col("population").desc)
  .show(10)
