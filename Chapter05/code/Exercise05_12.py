from pyspark.sql.functions import *

dfTran01 = dfToRdd04.select("city", "zip_code")
dfTran01.show()

dfTran02 = dfToRdd04.select(col("city"))
dfTran02.show()

dfTran03 = dfToRdd04.selectExpr("city", "to_date('20200901', 'yyyyMMdd') as today")
dfTran03.show()

dfTran03.select("today").distinct().show()

dfTran01.select("city", regexp_replace(col("city"), "M", "").alias("r_city")).show()

dfTran01.select("city", when(col("city") == "Phoenix", "true").when(col("city") == "Juneau", "true").otherwise("false").alias("visited_flag")).show()

dfTran04 = dfTran01.select("city", concat(lit("   "), col("city"), lit("   ")).alias("city_with_space"))
dfTran04.select("city", ltrim(col("city_with_space")).alias("city_ltrim"), rtrim(col("city_with_space")).alias("city_rtrim"), trim(col("city_with_space")).alias("city_trim")).show()

dfTran05 = dfTran01.select("city", "zip_code", lit(None).alias("null_col"))
dfTran05.select(coalesce("null_col", "zip_code", "city")).show()