:paste

val dfMysql01 = spark.read.format("jdbc")
   .option("url", "jdbc:mysql://localhost:port/database_name")
   .option("driver", "com.mysql.jdbc.Driver")
   .option("dbtable", "table_name") 
   .option("user", "user") 
   .option("password", "password") 
   .load()

   
