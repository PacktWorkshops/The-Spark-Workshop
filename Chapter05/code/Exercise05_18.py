gpa \
  .cube(col("school"), col("class")) \
  .count() \
  .sort(asc("school"), asc("class")) \
  .show() 

gpa \
  .rollup(col("school"), col("class")) \
  .count() \
  .sort(asc("school"), asc("class")) \
  .show()
