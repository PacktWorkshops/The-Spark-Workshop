// The Spark Workshop

// Chapter 5
// DataFrames with Spark

// By Craig Covey

// 1

val df = spark.read
  .format("csv")
  .option("sep", ",")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("hdfs://…/royal_holloway/Schools dispersals data with BHL links.csv")

// 2

df.printSchema

// 3

val scala_df = df
  .withColumnRenamed("EventDate", "event_date_string")
  .withColumnRenamed("Period", "period")
  .withColumnRenamed("Page number (if applic)", "page_number")
  .withColumnRenamed("Recipient name (original transcription)", "recipient_name")
  .withColumnRenamed("Nature of recipient (original transcription)", "formal_school_name")
  .withColumnRenamed("name", "name")
  .withColumnRenamed("Institution - common name from Exit Books", "school_name")
  .withColumnRenamed("Town/City", "city")
  .withColumnRenamed("Level of study ", "level_of_study")
  .withColumnRenamed("School type", "school_type")
  .withColumnRenamed("Denomination/organisation", "denomination_or_org")
  .withColumnRenamed("No. of objects", "object_count")
  .withColumnRenamed("Event information (any extra information not in list of specimens)", "event_info")
  .withColumnRenamed("BHL Exit Book Link", "book_link_1")
  .withColumnRenamed("BHL School Letter Book Link (Vol 1 only)", "book_link_2")


//4

scala_df.show()

// 5

val drop_df = scala_df.drop("book_link_1", "book_link_2")

// 6

import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.functions.to_date

val date_df = drop_df.withColumn("event_date", to_date(drop_df("event_date_string"), "dd.MM.yyyy"))


// 7

val select_df = date_df.select("event_date", "page_number", "recipient_name", "formal_school_name", "name", "school_name", "city", "level_of_study", "school_type", "denomination_or_org", "object_count")


// 8

val kew_df = select_df.orderBy(select_df("event_date").asc)


// 9

kew_df.count

// 10

kew_df.select(kew_df("city")).distinct.count

// 11

import org.apache.spark.sql.functions.col
kew_df.select(kew_df("city")).distinct.orderBy(col("city").asc).show(60)


// 12

import org.apache.spark.sql.functions.{sum, col}

kew_df.groupBy("level_of_study").agg(sum(kew_df("object_count"))).orderBy(col("sum(object_count)")).show()


// 13

import org.apache.spark.sql.functions.{avg, year}

kew_df.groupBy(year(kew_df("event_date"))).agg(avg(kew_df("page_number"))).show(43)

// 14

import org.apache.spark.sql.functions.min

kew_df.groupBy("school_type", "denomination_or_org").agg(min(kew_df("object_count")).alias("min_objects")).show(50)
