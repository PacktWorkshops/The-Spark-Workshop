dfCsv01 = spark.read.csv("Chapter05/data/zipcodes.csv")
 
dfCsv02 = spark.read.option("header", "true").csv("Chapter05/data/zipcodes.csv")
 
dfCsv03 = spark.read.format("csv").option("header", "true").load("Chapter05/data/zipcodes.csv")
 
dfCsv04 = spark.read.csv(["Chapter05/data/zipcodes.csv", "Chapter05/data/address.csv"])
 
dfCsv05 = spark.read.csv("Chapter05/data")
