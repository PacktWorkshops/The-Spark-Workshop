:paste
val sparkMongoSession = SparkSession.builder()
   .master("local")
   .appName("Mongo Spark Connector example")
   .config("spark.mongodb.input.uri","mongodb://127.0.0.1/test.zipcodes")
   .config("spark.mongodb.output.uri","mongodb://127.0.0.1/test.zipcodes")
   .getOrCreate()

val dfMongo01 = sparkMongoSession.read.format("mongo").load()