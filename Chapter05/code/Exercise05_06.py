spark = SparkSession \
  .builder \
  .appName("Spark Hive session") \
  .config("spark.sql.warehouse.dir", "/tmp/warehouse") \
  .enableHiveSupport() \
  .getOrCreate()

spark.sql("select * from zipcodes").show()
