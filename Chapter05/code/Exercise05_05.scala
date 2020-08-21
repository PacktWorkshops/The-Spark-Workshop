val dfPar01 = spark.read.parquet("Chapter05/data/zipcodes.parquet") 
val dfPar02 = spark.read.format("parquet").load("Chapter05/data/zipcodes.parquet")
