from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as F


# Function to read data in RDD
def read(row): print(
    "The " + row['name'] + ", in the " + row['category'] + " category, eats " + row['food'] + " most commonly.")


# Create a Spark Session
spark = SparkSession \
    .builder \
    .appName("My Spark App") \
    .master("local[2]") \
    .getOrCreate()

# create spark contexts
sc = spark.sparkContext

# create a dataset
my_previous_pets = [Row("fido", "dog", 4, "brown"),
                    Row("annabelle", "cat", 15, "white"),
                    Row("fred", "bear", 29, "brown"),
                    Row("gus", "parakeet", 2, "black"),
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

# spark.sql('select nickname, '
#           'count(*) as occurrences '
#           'from pets '
#           'group by nickname '
#           'order by occurrences desc '
#           'limit 3').show()
#
# petsDF.where("type = 'cat'")\
#     .agg({"age": "max"})\
#     .show()

# petsDF.where(petsDF["type"] == "cat") \
#   .groupBy("type") \
#   .agg(F.min("age"), F.max("age")) \
#   .show()
#
# petsDF.where("type = 'dog'")\
#     .groupBy("type")\
#     .agg(F.avg("age"))\
#     .show()

# petsDF.groupBy("color").count().show()