dfTran01.filter(col("city") == "Phoenix").show()
dfTran01.where(col("city") == "Phoenix").show()

dfTran01.filter((col("city") == "Phoenix") & (col("zip_code") == "85001")).show()
dfTran01.filter((col("city") == "Phoenix") | (col("city") == "Juneau")).show()
dfTran01.filter(~(col("city") == "Phoenix")).show()

dfTran06 = dfTran01.withColumn("array_col", array(col("city"), col("zip_code")))
dfTran06.filter(array_contains(col("array_col"), "Phoenix")).show()