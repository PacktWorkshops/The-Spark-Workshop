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

my_previous_pets = [Row("annabelle", "cat"),
                    Row("daisy", "kitten"),
                    Row("roger", "puppy"),
                    Row("joe", "puppy dog"),
                    Row("rosco", "dog"),
                    Row("julie", "feline")]

# create RDDs
petsRDD = sc.parallelize(my_previous_pets)

# create data frames
petsDF = spark.createDataFrame(petsRDD, ['nickname', 'type'])

dogs = petsDF.where(col("type").isin("dog", "puppy", "puppy dog", "hound", "canine"))
cats = petsDF.where(col("type").isin(["cat", "kitty", "kitten", "feline", "kitty cat"]))

dogs.show()
cats.show()
