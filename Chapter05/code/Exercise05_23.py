gpa.write.option("header","true").mode("overwrite").csv("Chapter05/data/output_gpa.csv")
gpa.write.format("csv").option("header","true").mode("overwrite").save("Chapter05/data/output_gpa.csv")

gpa.write.mode("overwrite").json("Chapter05/data/output_gpa.json")
gpa.write.format("json").mode("overwrite").save("Chapter05/data/output_gpa.json")

gpa.write.mode("overwrite").orc("Chapter05/data/output_gpa.orc")
gpa.write.format("orc").mode("overwrite").save("Chapter05/data/output_gpa.orc")

gpa.write.mode("overwrite").parquet("Chapter05/data/output_gpa.parquet")
gpa.write.format("parquet").mode("overwrite").save("Chapter05/data/output_gpa.parquet")

gpa.write.partitionBy("school").mode("overwrite").orc("Chapter05/data/output_gpa.orc")
