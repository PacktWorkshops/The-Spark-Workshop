dfZipCode = spark.read.option("header", "true").csv("Chapter05/data/zipcodes.csv")
dfPopulation = spark.read.option("header", "true").csv("Chapter05/data/population.csv")
dfZipCode.show()
dfPopulation.show()

dfZipCode.join(dfPopulation, dfZipCode.City == dfPopulation.City, "inner").show()

dfZipCode.join(dfPopulation, dfZipCode.City == dfPopulation.City, "left").show()

dfZipCode.join(dfPopulation, dfZipCode.City == dfPopulation.City, "right").show()

dfZipCode.join(dfPopulation, dfZipCode.City == dfPopulation.City, "full").show()

dfZipCode.crossJoin(dfPopulation).show(truncate=False)

dfZipCode.join(dfPopulation, dfZipCode.City == dfPopulation.City, "leftsemi").show()

dfZipCode.join(dfPopulation, dfZipCode.City == dfPopulation.City, "leftanti").show()

dfPopulation.alias("p1") \
.join(dfPopulation.alias("p2"), col("p1.City") == col("p2.City")) \
.join(dfPopulation.alias("p3"), col("p1.City") == col("p3.City")) \
.show()
