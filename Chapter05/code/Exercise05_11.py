dfToRdd04.show()
dfToRdd04.show(n=2, truncate=False)
dfToRdd04.show(vertical=True)

dfToRdd04.count()

dfToRdd04.columns

from pyspark.sql.functions import lit

dfToRdd05 = dfToRdd04.withColumn("valid", lit("true"))
dfToRdd05.show()

dfToRdd06 = dfToRdd05.withColumnRenamed("valid", "valid_flag")
dfToRdd06.show()

dfToRdd06.drop("valid_flag").show()