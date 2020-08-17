dfJson01 = spark.read.json("Chapter05/data/zipcodes.json")
dfJson02 = spark.read.format("json").load("Chapter05/data/zipcodes.json")
