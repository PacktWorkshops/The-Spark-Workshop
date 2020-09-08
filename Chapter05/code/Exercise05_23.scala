import org.apache.spark.sql.SaveMode

gpa.write.option("header","true").mode(SaveMode.Overwrite).csv("Chapter05/data/output_gpa.csv")

gpa.write.mode(SaveMode.Overwrite).json("Chapter05/data/output_gpa.json")
gpa.write.format("json").mode(SaveMode.Overwrite).save("Chapter05/data/output_gpa.json")

gpa.write.mode(SaveMode.Overwrite).orc("Chapter05/data/output_gpa.orc")
gpa.write.format("orc").mode(SaveMode.Overwrite).save("Chapter05/data/output_gpa.orc")

gpa.write.mode(SaveMode.Overwrite).parquet("Chapter05/data/output_gpa.parquet")
gpa.write.format("parquet").mode(SaveMode.Overwrite).save("Chapter05/data/output_gpa.parquet")

gpa.write.partitionBy("school").mode(SaveMode.Overwrite).orc("Chapter05/data/output_gpa.orc")
