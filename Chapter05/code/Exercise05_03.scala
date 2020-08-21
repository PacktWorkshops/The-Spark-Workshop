val dfJson01 = spark.read.json("Chapter05/data/zipcodes.json")
val dfJson02 = spark.read.format("json").load("Chapter05/data/zipcodes.json")
