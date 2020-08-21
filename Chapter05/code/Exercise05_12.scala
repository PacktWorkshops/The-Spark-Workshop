val dfTran01 = dfToRdd04.select($"city", $"zip_code")
dfTran01.show()

val dfTran02 = dfToRdd04.select(col("city"))
dfTran02.show()

val dfTran03 = dfToRdd04.selectExpr("city", "to_date('20200901', 'yyyyMMdd') as today")
dfTran03.show

dfTran03.select($"today").distinct().show()

dfTran01.select($"city", regexp_replace($"city", "M", "").as("r_city")).show()

dfTran01.select($"city", when($"city" === "Phoenix", "true").when($"city" === "Juneau", "true").otherwise("false").as("visited_flag")).show()

val dfTran04 = dfTran01.select($"city", concat(lit("   "), $"city", lit("   ")).as("city_with_space"))
dfTran04.select($"city", ltrim($"city_with_space").as("city_ltrim"), rtrim($"city_with_space").as("city_rtrim"), trim($"city_with_space").as("city_trim")).show()

val dfTran05 = dfTran01.select($"city", $"zip_code", lit(null).as("null_col"))
dfTran05.select(coalesce($"null_col", $"zip_code", $"city")).show()