pivotDF = gpa.groupBy("school").pivot("class").sum("gpa")
pivotDF.show()

unPivotDF = pivotDF.select(col("school"), expr("stack(3, '3A', 3A, '3B', 3B, '3C', 3C) as (class, sum_gpa)"))
unPivotDF.show()
