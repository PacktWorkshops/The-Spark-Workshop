from pyspark.sql.window import Window
val window = Window.partitionBy("school")

:paste
gpa
.withColumn("max", max(col("gpa")).over(window))
.withColumn("min", min(col("gpa")).over(window))
.withColumn("sum", sum(col("gpa")).over(window))
.withColumn("avg", avg(col("gpa")).over(window))
.withColumn("cnt", count(col("gpa")).over(window))
.show()

val windowOrdered = Window.partitionBy("school").orderBy("gpa")

:paste
gpa
.withColumn("row_num", row_number().over(windowOrdered))
.withColumn("rank", rank().over(windowOrdered))
.withColumn("dense_rank", dense_rank().over(windowOrdered))
.withColumn("percent_rank", percent_rank().over(windowOrdered))
.withColumn("ntile", ntile(3).over(windowOrdered))
.show()

:paste
gpa
.withColumn("cume_dist", cume_dist().over(windowOrdered))
.withColumn("lead", lead("gpa", 1, "empty").over(windowOrdered))
.withColumn("lag", lag("gpa", 1, "empty").over(windowOrdered))
.show()