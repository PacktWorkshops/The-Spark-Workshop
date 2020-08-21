spark = SparkSession \
   .builder \
   .appName("exercise_seven") \
   .getOrCreate()

home_computers = [["Honeywell", "Honeywell 316#Kitchen Computer", "DDP 16 Minicomputer", 1969], ["Apple Computer", "Apple II series", "6502", 1977], ["Bally Consumer Products", "Bally Astrocade", "Z80", 1977]]

computers_df = spark.createDataFrame(home_computers, ["Manufacturer", "Model", "Processor", "Year"])

computers_df.show()
computers_df.printSchema()
