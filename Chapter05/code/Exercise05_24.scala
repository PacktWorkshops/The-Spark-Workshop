gpa.write.mode(SaveMode.Overwrite).saveAsTable("gpa")

:paste
gpa.write.format("jdbc")
   .option("url", "jdbc:mysql://localhost:port/database_name")
   .option("driver", "com.mysql.jdbc.Driver")
   .option("dbtable", "table_name") 
   .option("user", "user") 
   .option("password", "password") 
   .mode(SaveMode.Overwrite)
   .load()

:paste
val sparkMongoSession = SparkSession.builder()
   .master("local")
   .appName("Mongo Spark Connector example")
   .config("spark.mongodb.input.uri","mongodb://127.0.0.1/test.gpa")
   .config("spark.mongodb.output.uri","mongodb://127.0.0.1/test.gpa")
   .getOrCreate()

sparkMongoSession.save(gpa.write.option("collection", "gpa").mode("overwrite"))
