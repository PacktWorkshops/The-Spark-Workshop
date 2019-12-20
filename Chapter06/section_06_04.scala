val spark = SparkSession
   .builder()
   .appName("some_app_name")
   .getOrCreate()

census_df.write.save("hdfs://…/some_parent_dir/another_directory")

// Exercise 8

census_df.write.parquet("hdfs://…/spark_output/parquet")

census_df.write.format("parquet").save("hdfs://…/spark_output/parquet2")

census_df.write.format("parquet").option("compression", "gzip").save("hdfs://…/spark_output/parquet3")

// Exercise 9

census_df.write.orc("hdfs://…/spark_output/orc")

census_df.write.format("orc").save("hdfs://…/spark_output/orc2")

census_df.write.format("orc").mode("append").save("hdfs://…/spark_output/orc2")

census_df.write.format("orc").mode("overwrite").save("hdfs://…/spark_output/orc2")

// Exercise 10

census_df.write.mode("overwrite").json("hdfs://…/spark_output/json")

census_df.write.format("json").mode("overwrite").option("dateFormat", "yyyy/mm/dd").option("lineSep", "\n").save("hdfs://…/spark_output/json")

// Exercise 11

census_df.write
  .mode("overwrite")
  .option("sep", ",")
  .option("quote", "")
  .option("header", "true")
  .csv("hdfs://…/spark_output/csv")

// Exercise 12

census_df.write
  .format("avro")
  .mode("append")
  .save("hdfs://…/spark_output/avro")



census_df.rdd.getNumPartitions


// Exercise 13

val four_parts_df = census_df.coalesce(4)
four_parts_df.rdd.getNumPartitions

val three_parts_df = census_df.repartition(3)
three_parts_df.rdd.getNumPartitions

val state_county_part = census_df.repartition(census_df("us_state"), census_df("county_name"))
state_county_part.rdd.getNumPartitions


census_df.write
  .format("orc")
  .mode("overwrite")
  .partitionBy("us_state", "county_name")
  .save("hdfs://…/spark_output/part_by_state_orc")
