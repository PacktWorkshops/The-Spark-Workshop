import spark.implicits._
val columns = Seq("city","zip_code")
val data = Seq(("Montgomery", "36104"),("Juneau", "99801"),("Phoenix", "85001"))
val rdd = spark.sparkContext.parallelize(data)

val dfToRdd01 = rdd.toDF()
val dfToRdd02 = rdd.toDF("city", "zip_code")
val dfToRdd03 = rdd.toDF(columns:_*)
dfToRdd01.printSchema()
dfToRdd02.printSchema()
dfToRdd03.printSchema()
