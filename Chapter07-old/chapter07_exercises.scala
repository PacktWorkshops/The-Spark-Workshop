// Section 7.3

val adult_cat_df = spark.read.format("csv")
  .option("sep", ",")
  .option("inferSchema", "true")
  .option("header", "false")
  .load("hdfs://.../adult/adult_data.csv")
  .toDF("age", "workclass", "fnlwgt", "education", "education-num", "marital-status", "occupation", "relationship", "race", "sex", "capital-gain", "capital-loss", "hours-per-week", "native-country", "class")
  .drop("fnlwgt", "education-num", "capital-gain", "capital-loss")


val adult_cat_df = (spark.read.format("csv")
  .option("sep", ",")
  .option("inferSchema", "true")
  .option("header", "false")
  .load("hdfs://.../adult/adult_data.csv")
  .toDF("age", "workclass", "fnlwgt", "education", "education-num", "marital-status", "occupation", "relationship", "race", "sex", "capital-gain", "capital-loss", "hours-per-week", "native-country", "class")
  .drop("fnlwgt", "education-num", "capital-gain", "capital-loss")
)


import org.apache.spark.sql.functions.trim

val trimmed_df = (adult_cat_df
  .withColumn("workclass", trim(adult_cat_df("workclass")))
  .withColumn("education", trim(adult_cat_df("education")))
  .withColumn("marital-status", trim(adult_cat_df("marital-status")))
  .withColumn("occupation", trim(adult_cat_df("occupation")))
  .withColumn("relationship", trim(adult_cat_df("relationship")))
  .withColumn("race", trim(adult_cat_df("race")))
  .withColumn("sex", trim(adult_cat_df("sex")))
  .withColumn("native-country", trim(adult_cat_df("native-country")))
  .withColumn("class", trim(adult_cat_df("class")))
)


val clean_df = (trimmed_df.dropDuplicates
  .na.replace("*", Map("?" -> null))
  .na.drop(minNonNulls = 9)
)



import org.apache.spark.sql.functions.avg

(clean_df
 .filter(clean_df("class") === ">50K")
 .filter(clean_df("age").between(19, 60))
 .groupBy("class", "age")
 .agg(avg(clean_df("hours-per-week")))
 .orderBy(clean_df("age").asc)
 .show(41)
)


val some_df = spark.sql("SELECT * FROM adult")
some_df.show


clean_df.createOrReplaceTempView("adult")


spark.sql("SELECT * FROM adult").show()


spark.sql("SHOW TABLES").show


clean_df.createOrReplaceGlobalTempView("global_adult")


spark.sql("SHOW TABLES IN global_temp").show()


spark.sql("SELECT * FROM global_temp.global_adult LIMIT 5").show()


spark.catalog.dropGlobalTempView("global_adult")


spark.catalog.dropTempView("adult")



spark.sql("""
SELECT
  age
  , occupation
  , `hours-per-week`
  , `native-country`
  , class
FROM adult
LIMIT 7
"""
).show()



spark.sql("""
SELECT
  age
  , occupation
  , `hours-per-week`
  , `native-country`
  , class
FROM adult
WHERE occupation = 'Sales'
LIMIT 7
"""
).show()



spark.sql("""
SELECT
  age
  , occupation
  , `hours-per-week`
  , `native-country`
  , class
FROM adult
WHERE occupation = 'Sales'
ORDER BY `hours-per-week` DESC
LIMIT 7
"""
).show()



spark.sql("""
SELECT
  AVG(age)
  , AVG(`hours-per-week`) AS avg_hrs_per_wk
FROM adult
"""
).show()


spark.sql("""
SELECT
  MAX(age) AS oldest_age
  , SUM(`hours-per-week`) AS total_hours
FROM adult
"""
).show()



spark.sql("""
SELECT
  `native-country`
  , AVG(age) AS avg_age
FROM adult
GROUP BY `native-country`
"""
).show()


spark.sql("""
SELECT
  `native-country`
  , class
  , MAX(`hours-per-week`) AS max_work
FROM adult
GROUP BY `native-country`, class
ORDER BY `native-country`
"""
).show()


spark.sql("""
SELECT
  `native-country`
  , class
  , MAX(`hours-per-week`) AS max_work
FROM adult
GROUP BY `native-country`, class
HAVING max_work >= 60
ORDER BY `native-country`
"""
).show()


// Exercise 7.02

spark.sql("SELECT * FROM adult").show()

spark.sql("""
SELECT *
FROM adult
WHERE education = 'Bachelors'
ORDER BY age ASC
LIMIT 15
"""
).show()


spark.sql("""
SELECT
  `marital-status`
  , sex
  , AVG(`hours-per-week`) AS avg_hours_worked
  , MAX(`hours-per-week`) AS max_hours_worked
FROM adult
WHERE age < 30
GROUP BY `marital-status`, sex
HAVING `marital-status` = 'Never-married'
ORDER BY avg_hours_worked DESC
"""
).show()


// Section 7.4

val airlines_df = (spark.read.format("csv")
  .option("sep", ",")
  .option("inferSchema", "true")
  .option("header", "false")
  .load("hdfs://.../openflights/airlines_data.csv")
  .toDF("airline_id", "airline_name", "alias", "iata", "icao-num", "callsign", "country", "active")
)

val routes_df = (spark.read.format("csv")
  .option("sep", ",")
  .option("inferSchema", "true")
  .option("header", "false")
  .load("hdfs://.../openflights/routes_data.csv")
  .toDF("airline", "airline_id", "source_airport", "source_airport_id", "dest_airport", "dest_airport_id", "codeshare", "stops", "equipment")
  .drop("codeshare", "stops", "equipment")
)


val airports_df = (spark.read.format("csv")
  .option("sep", ",")
  .option("inferSchema", "true")
  .option("header", "false")
  .load("hdfs://.../openflights/airports_data.csv")
  .toDF("airport_id", "airport_name", "airport_city", "airport_country", "iata", "icao", "latitude", "longitude", "altitude", "timezone", "dst", "tz_time", "type", "source")
  .drop("timezone", "dst", "tz_time", "type", "source")
)


airlines_df.createOrReplaceTempView("airlines")
routes_df.createOrReplaceTempView("routes")
airports_df.createOrReplaceTempView("airports")




spark.sql("""
SELECT *
FROM airlines
LIMIT 10
"""
).show()



spark.sql("""
SELECT *
FROM routes
LIMIT 10
"""
).show()


spark.sql("""
SELECT
  routes.airline_id
  , airlines.airline_name
  , routes.source_airport
  , routes.source_airport_id
  , routes.dest_airport
  , routes.dest_airport_id
FROM routes
LEFT JOIN airlines
  ON routes.airline_id = airlines.airline_id
"""
).show(10)


spark.sql("""
SELECT
  r.airline_id
  , a.airline_name
  , r.source_airport
  , r.source_airport_id
  , r.dest_airport
  , r.dest_airport_id
FROM routes r
LEFT JOIN airlines a
  ON r.airline_id = a.airline_id
"""
).show(10)



val routes_airline_df = routes_df.join(airlines_df, routes_df("airline_id") === airlines_df("airline_id"), "left")


val routes_airline_df = routes_df
  .join(airlines_df, routes_df("airline_id") === airlines_df("airline_id"), "left")
  .select(routes_df("airline_id"), airlines_df("airline_name"), routes_df("source_airport"), routes_df("source_airport_id"), routes_df("dest_airport"), routes_df("dest_airport_id"))



spark.sql("""
SELECT
  airlines.airline_name
  , routes.airline_id
  , routes.source_airport
  , routes.source_airport_id
  , airports.airport_name AS source_airport_name
  , airports.airport_city AS source_airport_city
  , airports.airport_country AS source_airport_country
  , routes.dest_airport
  , routes.dest_airport_id
FROM routes
LEFT JOIN airlines
ON routes.airline_id = airlines.airline_id
LEFT JOIN airports
ON routes.source_airport_id = airports.airport_id
"""
).show()



spark.sql("""
SELECT
  air.airline_name
  , r.airline_id
  , r.source_airport
  , r.source_airport_id
  , source_air.airport_name AS source_airport_name
  , source_air.airport_city AS source_airport_city
  , source_air.airport_country AS source_airport_country
  , r.dest_airport
  , r.dest_airport_id
  , dest_air.airport_name AS dest_airport_name
  , dest_air.airport_city AS dest_airport_city
  , dest_air.airport_country AS dest_airport_country
FROM routes r
LEFT JOIN airlines air
ON r.airline_id = air.airline_id
LEFT JOIN airports source_air
ON r.source_airport_id = source_air.airport_id
LEFT JOIN airports dest_air
ON r.dest_airport_id = dest_air.airport_id
"""
).show()


val new_dataframe_name_1 = df.alias("new_dataframe_name_1")
val new_dataframe_name_2 = df.alias("new_dataframe_name_2")



import org.apache.spark.sql.functions.col

val source_air = airports_df.alias("source_air")
val dest_air = airports_df.alias("dest_air")

val routes_airline_df = (
  routes_df
    .join(airlines_df, routes_df("airline_id") === airlines_df("airline_id"), "left")
    .join(source_air, routes_df("source_airport_id") === source_air("airport_id"), "left")
    .join(dest_air, routes_df("dest_airport_id") === col("dest_air.airport_id"), "left")
    .select(airlines_df("airline_name")
            , routes_df("airline_id")
            , routes_df("source_airport")
            , routes_df("source_airport_id")
            , source_air("airport_name").alias("source_airport_name"), source_air("airport_city").alias("source_airport_city"), source_air("airport_country").alias("source_airport_country")
            , routes_df("dest_airport"), routes_df("dest_airport_id")
            , col("dest_air.airport_name").alias("dest_airport_name"), col("dest_air.airport_city").alias("dest_airport_city"), col("dest_air.airport_country").alias("dest_airport_country")
     )
)


spark.sql("""
  WITH alt AS (
      SELECT
        *
      FROM airports
    )
  SELECT *
  FROM alt
""").show()



spark.conf.set("spark.sql.crossJoin.enabled", true)

spark.sql("""
  WITH total_alt AS (
      SELECT
        SUM(altitude) AS sum_altitude
      FROM airports
    ),
    total_rows AS (
      SELECT
        COUNT(*) AS row_count
      FROM airports
    )
  SELECT
    total_alt.sum_altitude / total_rows.row_count AS avg_altitude
  FROM total_alt, total_rows
""").show()



spark.sql("""
  SELECT
    AVG(altitude) AS avg_altitude
  FROM airports
""").show()


spark.sql("""
  WITH air_table AS (
    SELECT
      airlines.airline_name
      , routes.airline_id
      , routes.source_airport
      , routes.source_airport_id
      , airports.airport_name AS source_airport_name
      , airports.airport_city AS source_airport_city
      , airports.airport_country AS source_airport_country
      , routes.dest_airport
      , routes.dest_airport_id
    FROM routes
    LEFT JOIN airlines
    ON routes.airline_id = airlines.airline_id
    LEFT JOIN airports
    ON routes.source_airport_id = airports.airport_id
  )
  SELECT
      air_table.*
    , airports.airport_name AS dest_airport_name
    , airports.airport_city AS dest_airport_city
    , airports.airport_country AS dest_airport_country
  FROM air_table
  LEFT JOIN airports
    ON air_table.dest_airport_id = airports.airport_id
"""
).show()



spark.sql("""
  SELECT *
  FROM airlines
  WHERE airline_id < 10
  UNION ALL
  SELECT *
  FROM airlines
  WHERE airline_id > 990
    AND airline_id <= 1000
""").show()



spark.sql("""
  SELECT
    ports.icao
    , ports.airport_name AS type
  FROM airports ports
  UNION
  SELECT
    lines.`icao-num`
    , lines.airline_name
  FROM airlines lines
  WHERE lines.`icao-num` != 'null'
  ORDER BY ports.icao
""").show(truncate = false)



// Section 7.5


spark.sql("DROP TABLE database_name.table_name")

spark.sql("DROP TABLE IF EXISTS database_name.table_name")

scala> spark.conf.get("spark.sql.warehouse.dir")

spark.sql("""
CREATE TABLE IF NOT EXISTS airlines_table
USING PARQUET
AS SELECT * FROM airlines
""")

spark.sql("SHOW TABLES").show()

spark.sql("SELECT COUNT(*) FROM default.airlines_table").show()


spark.sql("""
CREATE TABLE default.airports_table
  (airport_id INT, airport_name STRING, airport_city STRING, airport_country STRING, iata STRING, icao STRING, latitude DOUBLE, longitude DOUBLE, altitude INT)
  USING PARQUET
""")

import org.apache.spark.sql.functions.col
spark.sql("SHOW TABLES").filter(col("tableName").startsWith("a")).show()

spark.sql("SELECT * FROM default.airports_table").show()

spark.sql("INSERT INTO default.airports_table SELECT * from airports")

spark.sql("LOAD DATA INPATH '/some/directory/airports_data.parquet' INTO TABLE default.airports_table")

spark.sql("SELECT * FROM default.airports_table LIMIT 10").show()

airports_df.write.saveAsTable("airports_table")

spark.sql("DROP TABLE database_name.table_name")

spark.sql("CREATE DATABASE IF NOT EXISTS aviation_data")

spark.sql("SHOW DATABASES").show()

spark.sql("DROP DATABASE IF EXISTS aviation_data")


spark.sql("""
CREATE TABLE IF NOT EXISTS aviation_data.routes_table
(airline STRING, airline_id STRING, source_airport STRING, source_airport_id STRING, dest_airport STRING, dest_airport_id STRING)
USING PARQUET
LOCATION '/path/to/a/directory/'
""")


spark.sql("""
CREATE TABLE IF NOT EXISTS aviation_data.routes_table
(airline STRING, airline_id STRING, source_airport STRING, source_airport_id STRING, dest_airport STRING, dest_airport_id STRING)
USING PARQUET
OPTIONS (PATH '/path/to/a/directory/')
""")


spark.sql("SHOW TABLES IN aviation_data").show()


routes_df.write.option('path', "/path/to/a/directory/").saveAsTable("routes_table")


spark.sql("CREATE VIEW aviation_data.routes_view AS SELECT * FROM aviation_data.routes_table")


spark.sql("SHOW TABLES IN aviation_data").show()


// Activity Assignment 1

val airlines_df = (spark.read.format("csv")
  .option("sep", ",")
  .option("inferSchema", "true")
  .option("header", "false")
  .load("hdfs://.../openflights/airlines_data.csv")
  .toDF("airline_id", "airline_name", "alias", "iata", "icao-num", "callsign", "country", "active")
)

val routes_df = (spark.read.format("csv")
  .option("sep", ",")
  .option("inferSchema", "true")
  .option("header", "false")
  .load("hdfs://.../openflights/routes_data.csv")
  .toDF("airline", "airline_id", "source_airport", "source_airport_id", "dest_airport", "dest_airport_id", "codeshare", "stops", "equipment")
  .drop("codeshare", "stops", "equipment")
)


val airports_df = (spark.read.format("csv")
  .option("sep", ",")
  .option("inferSchema", "true")
  .option("header", "false")
  .load("hdfs://.../openflights/airports_data.csv")
  .toDF("airport_id", "airport_name", "airport_city", "airport_country", "iata", "icao", "latitude", "longitude", "altitude", "timezone", "dst", "tz_time", "type", "source")
  .drop("timezone", "dst", "tz_time", "type", "source")
)


airlines_df.createOrReplaceTempView("airlines")
routes_df.createOrReplaceTempView("routes")
airports_df.createOrReplaceTempView("airports")


spark.sql("""
SELECT
  dest_air.airport_name AS dest_airport_name
  , r.dest_airport
  , dest_air.airport_city AS dest_airport_city
  , dest_air.airport_country AS dest_airport_country
  , COUNT(air.airline_name) AS LAX_flight_count
FROM routes r
LEFT JOIN airlines air
ON r.airline_id = air.airline_id
LEFT JOIN airports source_air
ON r.source_airport_id = source_air.airport_id
LEFT JOIN airports dest_air
ON r.dest_airport_id = dest_air.airport_id
WHERE r.source_airport = "LAX"
GROUP BY r.dest_airport, dest_airport_name, dest_airport_city, dest_airport_country
HAVING LAX_flight_count > 1
ORDER BY LAX_flight_count DESC
""").show(truncate = false)



spark.conf.set("spark.sql.crossJoin.enabled", true)

spark.sql("""
  WITH lax_coordinates AS (
    SELECT
      a.latitude
      , a.longitude
    FROM airports a
    WHERE a.iata == "LAX"
  )
  SELECT
    ports.*,
    ROUND( 3959 * acos( cos( radians(lax_coordinates.latitude) )
        * cos( radians( ports.latitude ) )
        * cos( radians( ports.longitude ) - radians(lax_coordinates.longitude) )
        + sin( radians(lax_coordinates.latitude) )
        * sin( radians( ports.latitude ) ) ) , 2) AS distance_to_LAX
  FROM airports AS ports, lax_coordinates
  WHERE ports.airport_country = "United States"
  ORDER BY distance_to_LAX ASC
""").show(truncate = false)
