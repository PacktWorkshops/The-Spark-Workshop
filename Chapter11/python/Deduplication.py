from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import SQLContext


# Function to read data in RDD
def read(animal): print(animal['name'], animal['category'])


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

animalsDF.foreach(read)

deduplicated = animalsDF.dropDuplicates()
deduplicated.foreach(read)
