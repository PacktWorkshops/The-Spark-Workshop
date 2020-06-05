from pyspark.sql import SparkSession

# Create a Spark Session
spark = SparkSession\
    .builder\
    .appName("My Spark App")\
    .master("local[2]")\
    .getOrCreate()

sc = spark.sparkContext

# create a sample set of data as an RDD
categorized_animals = [("dog", "pet"), ("cat", "pet"), ("bear", "wild"), ("cat", "pet"), ("cat", "pet")]
animalDataRDD = sc.parallelize(categorized_animals)

# turn that RDD into a DataFrame
animalsDF = spark.createDataFrame(animalDataRDD, ['name', 'category'])

# Filter out animal names that start with 'c'
nonCats = animalsDF.filter("name not like 'c%'")
nonCats.show()

# Filter out animal names that are not 'cat'
nonCatsTwo = animalsDF.where("name != 'cat'")
nonCatsTwo.show()

# Filter out animals that are not in the 'pet' category
nonPets = animalsDF.filter("category != 'pet'")
nonPets.show()
