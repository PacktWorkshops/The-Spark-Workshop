val dfCsv01 = spark.read.csv("Chapter05/data/zipcodes.csv")

val dfCsv02 = spark.read.option("header", "true").csv("Chapter05/data/zipcodes.csv")

val dfCsv03 = spark.read.format("csv").option("header", "true").load("Chapter05/data/zipcodes.csv")

val dfCsv04 = spark.read.csv("Chapter05/data/zipcodes.csv", "Chapter05/data/address.csv")

val dfCsv05 = spark.read.csv("Chapter05/data")
