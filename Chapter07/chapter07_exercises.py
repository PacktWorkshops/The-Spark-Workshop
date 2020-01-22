## Section  7.3

adult_cat_df = spark.read.format("csv") \
  .load("hdfs://.../adult/adult_data.csv"
       , sep = ","
       , inferSchema = "true"
       , header = "false") \
  .toDF("age", "workclass", "fnlwgt", "education", "education-num", "marital-status", "occupation", "relationship", "race", "sex", "capital-gain", "capital-loss", "hours-per-week", "native-country", "class") \
  .drop("fnlwgt", "education-num", "capital-gain", "capital-loss")

adult_cat_df = (spark.read.format("csv")
  .load("hdfs://.../adult/adult_data.csv"
       , sep = ","
       , inferSchema = "true"
       , header = "false")
  .toDF("age", "workclass", "fnlwgt", "education", "education-num", "marital-status", "occupation", "relationship", "race", "sex", "capital-gain", "capital-loss", "hours-per-week", "native-country", "class")
  .drop("fnlwgt", "education-num", "capital-gain", "capital-loss")
)


from pyspark.sql.functions import trim

trimmed_df = (adult_cat_df
  .withColumn("workclass", trim(adult_cat_df["workclass"]))
  .withColumn("education", trim(adult_cat_df["education"]))
  .withColumn("marital-status", trim(adult_cat_df["marital-status"]))
  .withColumn("occupation", trim(adult_cat_df["occupation"]))
  .withColumn("relationship", trim(adult_cat_df["relationship"]))
  .withColumn("race", trim(adult_cat_df["race"]))
  .withColumn("sex", trim(adult_cat_df["sex"]))
  .withColumn("native-country", trim(adult_cat_df["native-country"]))
  .withColumn("class", trim(adult_cat_df["class"]))
)


clean_df = (trimmed_df.dropDuplicates()
            .replace("?", None)
            .dropna(thresh = 9)
)


from pyspark.sql import functions as F

(clean_df
 .filter(clean_df["class"] == ">50K")
 .filter(clean_df["age"].between(19, 60))
 .groupBy("class", "age")
 .agg(F.avg(clean_df["hours-per-week"]))
 .orderBy(clean_df["age"].asc())
 .show(41)
)


some_df = spark.sql("SELECT * FROM adult")
some_df.show()


clean_df.createOrReplaceTempView("adult")


spark.sql("SELECT * FROM adult").show()


clean_df.createOrReplaceGlobalTempView("global_adult")


spark.sql("SHOW TABLES IN global_temp").show()


spark.sql("SELECT * FROM global_temp.global_adult LIMIT 5").show()


spark.catalog.dropGlobalTempView("global_adult")


spark.catalog.dropTempView("adult")





## Section 7.4

airlines_df = (spark.read.format("csv")
  .load("hdfs://.../openflights/airlines_data.csv"
       , sep = ","
       , inferSchema = "true"
       , header = "false")
  .toDF("airline_id", "airline_name", "alias", "iata", "icao-num", "callsign", "country", "active")
)


routes_df = (spark.read.format("csv")
  .load("hdfs://.../openflights/routes_data.csv"
       , sep = ","
       , inferSchema = "true"
       , header = "false")
  .toDF("airline", "airline_id", "source_airport", "source_airport_id", "dest_airport", "dest_airport_id", "codeshare", "stops", "equipment")
  .drop("codeshare", "stops", "equipment")
)


airports_df = (spark.read.format("csv")
  .load("hdfs://.../openflights/airports_data.csv"
       , sep = ","
       , inferSchema = "true"
       , header = "false")
  .toDF("airport_id", "airport_name", "airport_city", "airport_country", "iata", "icao", "latitude", "longitude", "altitude", "timezone", "dst", "tz_time", "type", "source")
  .drop("timezone", "dst", "tz_time", "type", "source")
)

airlines_df.createOrReplaceTempView("airlines")
routes_df.createOrReplaceTempView("routes")
airports_df.createOrReplaceTempView("airports")



routes_airline_df = routes_df.join(airlines_df, routes_df["airline_id"] == airlines_df["airline_id"], "left")


routes_airline_df = (routes_df
  .join(airlines_df, routes_df["airline_id"] == airlines_df["airline_id"], "left")
  .select(routes_df["airline_id"], "airline_name", "source_airport", "source_airport_id", "dest_airport", "dest_airport_id")
)



new_dataframe_name_1 = df.alias("new_dataframe_name_1")
new_dataframe_name_2 = df.alias("new_dataframe_name_2")



from pyspark.sql.functions import col

source_air = airports_df.alias("source_air")
dest_air = airports_df.alias("dest_air")



routes_airline_df = (
  routes_df
    .join(airlines_df, routes_df["airline_id"] == airlines_df["airline_id"], "left")
    .join(source_air, routes_df["source_airport_id"] == source_air["airport_id"], "left")
    .join(dest_air, routes_df["dest_airport_id"] == col("dest_air.airport_id"), "left")
    .select(airlines_df["airline_name"]
            , routes_df["airline_id"]
            , routes_df["source_airport"]
            , routes_df["source_airport_id"]
            , source_air["airport_name"].alias("source_airport_name"), source_air["airport_city"].alias("source_airport_city"), source_air["airport_country"].alias("source_airport_country")
            , routes_df["dest_airport"], routes_df["dest_airport_id"]
            , col("dest_air.airport_name").alias("dest_airport_name"), col("dest_air.airport_city").alias("dest_airport_city"), col("dest_air.airport_country").alias("dest_airport_country")
     )
)
