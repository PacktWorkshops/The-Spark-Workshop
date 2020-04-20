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
           Row(7, "dog", "spotted", 7),
           Row(8, "dog", "brown", 1),
           Row(9, "dog", "brown", 20),
           Row(10, "dog", "brown", 3),
           Row(11, "dog", "brown", 7),
           Row(12, "dog", "brown", 9),
           Row(13, "dog", "brown", 10),
           Row(14, "dog", "brown", 3),
           Row(15, "dog", "brown", 6),
           Row(16, "dog", "brown", 13),
           Row(17, "dog", "brown", 4),
           Row(18, "dog", "brown", 5),
           Row(19, "dog", "brown", 7),
           Row(20, "dog", "brown", 8)]

# create RDDs
clientsRDD = sc.parallelize(clients)

# create data frames
clientsDF = spark.createDataFrame(clientsRDD, ['id', 'type', 'color', 'age'])
clientsDF.registerTempTable('dogs')

# slower due to skew in the 'brown' partition
spark.sql("select cast(avg(age) as INT) as average_age, color "
          "from dogs "
          "group by color "
          "order by average_age desc") \
    .show()

# faster due to repartitioning and splitting data for processing
brownDogs = clientsDF.where("color = 'brown'").repartition(10)
otherDogs = clientsDF.where("color != 'brown'")

brownDogsAvgAge = brownDogs.groupBy("color").avg("age")
otherDogsAvgAge = otherDogs.groupBy("color").avg("age")

combined = otherDogsAvgAge.union(brownDogsAvgAge)
combined.show()
