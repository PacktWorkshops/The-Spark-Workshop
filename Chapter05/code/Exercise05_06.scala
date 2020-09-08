val spark = SparkSession.builder().appName("Spark Hive session").config("spark.sql.warehouse.dir", "/tmp/warehouse").enableHiveSupport().getOrCreate()

val dfHive = spark.sql("select * from zipcodes")
dfHive.show()
