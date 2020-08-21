dfToRdd04.show
dfToRdd04.show(2, false)
dfToRdd04.count
dfToRdd04.columns

val dfToRdd05 = dfToRdd04.withColumn("valid", lit("true"))
dfToRdd05.show

val dfToRdd06 = dfToRdd05.withColumnRenamed("valid", "valid_flag")
dfToRdd06.show

dfToRdd06.drop("valid_flag").show
