from pyspark.sql import Row
rowRDD = map(lambda x: Row(*x), data)

from pyspark.sql.types import StructType, StructField, StringType
schema = StructType([StructField("city", StringType(), True), StructField("zip_code", StringType(), True)])

dfToRdd04 = spark.createDataFrame(rowRDD, schema)
dfToRdd04.printSchema()
