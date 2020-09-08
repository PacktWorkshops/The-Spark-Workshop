gpaRaw = spark.read.option("header", "true").csv("Chapter05/data/grade3_gpa.csv")
gpa = gpaRaw.selectExpr("student_name", "school", "class", "cast(gpa as decimal(2,1)) as gpa")
gpa.show()
gpa.printSchema()

gpa.groupBy("school").count().show()
gpa.groupBy().count().show()

gpa.groupBy("school", "class").count().show()

gpa.groupBy("school") \
.agg( \
      sum("gpa").alias("sum_gpa"), \
      avg("gpa").alias("avg_gpa"), \
      max("gpa").alias("max_gpa"), \
      min("gpa").alias("min_gpa"), \
      mean("gpa").alias("mean_gpa")) \
.where(col("avg_gpa") > 4.2) \
.show()

