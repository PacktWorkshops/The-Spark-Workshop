score = spark.read.option("header", "true").csv("Chapter05/data/sample_data_math_score.csv")
score.printSchema()

cla = spark.read.json("Chapter05/data/sample_data_school.json")
cla.printSchema()

score_not_null = score.where(col("math_score").isNotNull())

core_fn = score_not_null.withColumn("full_name", concat_ws(", ", col("last_name"), col("first_name")))

core_grade = core_fn.withColumn("grade", \
when((col("math_score") <= 100) & (col("math_score") >=90), "A+") \
.when((col("math_score") <= 89) & (col("math_score") >=85), "A") \
.when((col("math_score") <= 84) & (col("math_score") >=80), "A-") \
.when((col("math_score") <= 79) & (col("math_score") >=77), "B+") \
.when((col("math_score") <= 76) & (col("math_score") >=73), "B") \
.when((col("math_score") <= 72) & (col("math_score") >=70), "B-") \
.when((col("math_score") <= 69) & (col("math_score") >=65), "C+") \
.when((col("math_score") <= 64) & (col("math_score") >=60), "C") \
.when((col("math_score") <= 59) & (col("math_score") >=50), "D") \
.otherwise("F")).selectExpr("full_name", "class_id", "grade", "cast(math_score as int) as math_score")

core_grade.show(truncate=False)

score_school = core_grade.join(cla, core_grade.class_id == cla.class_id).drop("class_id")
score_school.show(truncate=False)

score_school.groupBy("class_number", "school").avg("math_score").show(truncate=False)
score_school.groupBy("school").avg("math_score").show(truncate=False)

score_school \
.withColumn("rank",rank().over(Window.orderBy(col("math_score").desc()))) \
.where(col("rank") <= 10) \
.show(truncate=False)