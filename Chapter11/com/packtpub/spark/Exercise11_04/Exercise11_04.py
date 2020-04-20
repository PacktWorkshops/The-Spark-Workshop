from pyspark.sql import SparkSession
from pyspark.sql import Row


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


def read(row): print(
    "The " + row['name'] + ", in the " + row['category'] + " category, eats " + row['food'] + " most commonly.")


# create a sample set of data as an RDD
categorized_animals = [("dog", "pet"), ("cat", "pet"), ("bear", "wild")]
animal_foods = [("dog", "kibble"), ("cat", "canned tuna"), ("bear", "salmon")]

# create RDDs
animalDataRDD = sc.parallelize(categorized_animals)
animalFoodRDD = sc.parallelize(animal_foods)

# create data frames
animalData = spark.createDataFrame(animalDataRDD, ['name', 'category'])
animalFoods = spark.createDataFrame(animalFoodRDD, ['animal', 'food'])

animals_enhanced = animalData.join(animalFoods, animalData.name == animalFoods.animal)
animals_enhanced = animalData.join(animalFoods, on=['name', 'category'])
animals_enhanced.foreach(read)
