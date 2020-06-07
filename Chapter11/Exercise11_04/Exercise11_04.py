from pyspark.sql import SparkSession

# Create a Spark Session
spark = SparkSession \
    .builder \
    .appName("My Spark App") \
    .master("local[2]") \
    .getOrCreate()

# create spark contexts
sc = spark.sparkContext

# create a sample set of data as an RDD
categorized_animals = [("dog", "pet"), ("cat", "pet"), ("bear", "wild")]
animal_foods = [("dog", "kibble"), ("cat", "canned tuna"), ("bear", "salmon")]

# create RDDs
animalDataRDD = sc.parallelize(categorized_animals)
animalFoodRDD = sc.parallelize(animal_foods)

# create data frames
animalData = spark.createDataFrame(animalDataRDD, ['name', 'category'])
animalFoods = spark.createDataFrame(animalFoodRDD, ['animal', 'food'])

# join them
animals_enhanced = animalData.join(animalFoods, animalData.name == animalFoods.animal)
animals_enhanced.show()
