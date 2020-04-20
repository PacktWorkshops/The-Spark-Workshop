from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import SQLContext


# Function to read data in RDD
def read(animal): print("I am no pet, for I am a " + animal['category'], animal['name'] + "!")


# Function to showcase filtered data
def read_non_cats(animal): print("I am a " + animal['name'] + ", which does not start with a 'c'.")


# Create a Spark Session
spark = SparkSession\
    .builder\
    .appName("My Spark App")\
    .master("local[2]")\
    .getOrCreate()

sc = spark.sparkContext
sqlContext = SQLContext(sc)

# create a sample set of data as an RDD
categorized_animals = [("dog", "pet"), ("cat", "pet"), ("bear", "wild"), ("cat", "pet"), ("cat", "pet")]
animalDataRDD = sc.parallelize(categorized_animals)
animals = animalDataRDD.map(lambda x: Row(name=x[0], category=x[1]))
animalsDF = sqlContext.createDataFrame(animals)

# Filter out animal names that start with 'c'
nonCats = animalsDF.filter("name not like 'c%'")
nonCats.foreach(read_non_cats)

# Filter out animal names that are not 'cat'
nonCatsTwo = animalsDF.where("name != 'cat'")

# Filter out animals that are not in the 'pet' category
nonPets = animalsDF.filter("category != 'pet'")
nonPets.foreach(read)
