dfTran01.filter(col("city") === "Phoenix").show()
dfTran01.filter(dfTran01("city") === "Phoenix").show()
dfTran01.filter($"city" === "Phoenix").show()

dfTran01.where(col("city") === "Phoenix").show()
dfTran01.where(dfTran01("city") === "Phoenix").show()
dfTran01.where($"city" === "Phoenix").show()

dfTran01.filter(col("city") === "Phoenix" && col("zip_code") === "85001").show()
dfTran01.filter(col("city") === "Phoenix" || col("city") === "Juneau").show()
dfTran01.filter(!(col("city") === "Phoenix")).show()

val dfTran06 = dfTran01.withColumn("array_col", array(col("city"), col("zip_code")))
dfTran06.filter(array_contains(col("array_col"), "Phoenix")).show()