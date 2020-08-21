columns = ["city","zip_code"]
data = [("Montgomery", "36104"),("Juneau", "99801"),("Phoenix", "85001")]
rdd = spark.sparkContext.parallelize(data)

dfToRdd01 = rdd.toDF()
dfToRdd02 = rdd.toDF(columns)
dfToRdd01.printSchema()
dfToRdd02.printSchema()
