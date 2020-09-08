val gpaRaw = spark.read.option("header", "true").csv("Chapter05/data/grade3_gpa.csv")
val gpa = gpaRaw.selectExpr("student_name", "school", "class", "cast(gpa as decimal(2,1)) as gpa")
gpa.show()
gpa.printSchema()

gpa.groupBy("school").count().show()
gpa.groupBy().count().show()

gpa.groupBy("school", "class").count().show()

:paste
gpa.groupBy("school")
.agg(
      sum("gpa").as("sum_gpa"),
      avg("gpa").as("avg_gpa"),
      max("gpa").as("max_gpa"),
      min("gpa").as("min_gpa"),
      mean("gpa").as("mean_gpa"))
.where(col("avg_gpa") > 4.2)
.show()

