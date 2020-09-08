val dfPopulation1 = spark.read.option("header", "true").csv("Chapter05/data/population.csv")
val dfPopulation2 = spark.read.option("header", "true").csv("Chapter05/data/population_add.csv")
dfPopulation1.show()
dfPopulation2.show()

dfPopulation1.union(dfPopulation2).show()

dfPopulation1.unionByName(dfPopulation2).show()

dfPopulation1.unionAll(dfPopulation2).distinct().show()

dfPopulation1.intersect(dfPopulation2).show()

dfPopulation1.intersectAll(dfPopulation2).show()

dfPopulation1.except(dfPopulation2).show()

dfPopulation1.exceptAll(dfPopulation2).show()

