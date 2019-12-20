from pyspark.sql import SparkSession


# Function to read data in RDD
def read(animal): print("I am now a distributed " + animal + "!")


# Create a Spark Session
spark = SparkSession\
    .builder\
    .appName("My Spark App")\
    .master("local[2]")\
    .getOrCreate()

# create a sample set of data as an RDD
animals = spark.sparkContext.parallelize(["dog", "cat", "bear"])

# apply a function on the RDD
animals.foreach(read)
