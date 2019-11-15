from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Create a Spark Session
spark = SparkSession \
    .builder \
    .appName("My Spark App") \
    .master("local[2]") \
    .getOrCreate()

# create spark contexts
sc = spark.sparkContext

my_previous_pets = [Row("fido", "dog", 4, "brown"),
                    Row("annabelle", "cat", 15, "white"),
                    Row("fred", "bear", 29, "brown"),
                    Row("daisy", "cat", 8, "black"),
                    Row("jerry", "cat", 1, "white"),
                    Row("fred", "parrot", 1, "brown"),
                    Row("gus", "fish", 1, "gold"),
                    Row("gus", "dog", 11, "black"),
                    Row("daisy", "iguana", 2, "green"),
                    Row("rufus", "dog", 10, "gold")]

# create RDDs
petsRDD = sc.parallelize(my_previous_pets)

# create data frames
petsDF = spark.createDataFrame(petsRDD, ['nickname', 'type', 'age', 'color'])
petsDF.registerTempTable('pets')

window = Window.partitionBy("type").orderBy(col("age").desc())

petsDF.withColumn("row_number", F.row_number().over(window))\
    .withColumnRenamed("row_number", "rank")\
    .where("rank <= 2 and (type = 'dog' or type = 'cat')")\
    .orderBy("type", "nickname")\
    .show()

