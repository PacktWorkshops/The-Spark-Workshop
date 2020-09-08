dfTran01.sort("city").show()
dfTran01.sort($"city").show()
dfTran01.sort(col("city")).show()

dfTran01.orderBy("city").show()
dfTran01.orderBy($"city").show()
dfTran01.orderBy(col("city")).show()

dfTran01.sort("city", "zip_code").show()

dfTran01.sort(col("city").asc).show()
dfTran01.sort(asc("city")).show()

dfTran01.sort(col("city").desc).show()
dfTran01.sort(desc("city")).show()