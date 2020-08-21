import org.apache.spark.sql.Row
val rowRDD = rdd.map(record => Row(record._1, record._2))

val schema = StructType(columns.map(fieldName => StructField(fieldName, StringType, nullable = true)))

val dfToRdd04 = spark.createDataFrame(rowRDD, schema)
dfToRdd04.printSchema()
