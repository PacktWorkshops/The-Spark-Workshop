val dfZipCode = spark.read.option("header", "true").csv("Chapter05/data/zipcodes.csv")
val dfPopulation = spark.read.option("header", "true").csv("Chapter05/data/population.csv")

dfZipCode.show()
dfPopulation.show()

dfZipCode.join(dfPopulation, dfZipCode("City") === dfPopulation("City"), "inner").show(false)

dfZipCode.join(dfPopulation, dfZipCode("City") === dfPopulation("City"), "left").show(false)

dfZipCode.join(dfPopulation, dfZipCode("City") === dfPopulation("City"), "right").show(false)

dfZipCode.join(dfPopulation, dfZipCode("City") === dfPopulation("City"), "full").show(false)

dfZipCode.crossJoin(dfPopulation).show(false)

dfZipCode.join(dfPopulation, dfZipCode("City") === dfPopulation("City"), "leftsemi").show(false)

dfZipCode.join(dfPopulation, dfZipCode("City") === dfPopulation("City"), "leftanti").show(false)

dfPopulation.alias("p1").join(dfPopulation.alias("p2"), col("p1.City") === col("p2.City")).join(dfPopulation.alias("p3"), col("p1.City") === col("p3.City")).show(false)
