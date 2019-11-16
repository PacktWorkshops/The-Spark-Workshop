from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col

# Create a Spark Session
spark = SparkSession \
    .builder \
    .appName("My Spark App") \
    .master("local[2]") \
    .getOrCreate()

# create spark contexts
sc = spark.sparkContext

clients = [Row(1, "dog", "brown", 1),
           Row(1, "dog", "brown", 2),
           Row(3, "dog", "white", 6),
           Row(3, "dog", "white", 8),
           Row(4, "dog", "black", 4),
           Row(4, "dog", "black", 14),
           Row(5, "dog", "red", 11),
           Row(6, "dog", "gold", 9),
           Row(6, "dog", "gold", 5),
           Row(7, "dog", "spotted", 7)]

# create RDDs
clientsRDD = sc.parallelize(clients)

# create data frames
clientsDF = spark.createDataFrame(clientsRDD, ['id', 'type', 'color', 'age'])
clientsDF.registerTempTable('dogs')

# Query the data
spark.sql("select cast(avg(age) as INT) as average_age, color "
          "from dogs "
          "group by color "
          "order by average_age desc")\
    .show()
