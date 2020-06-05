from pyspark.sql import SparkSession
from pyspark.sql import Row

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

# turn that RDD into a Dataframe and print a sample of the data
animalsDF = spark.createDataFrame(animalDataRDD, ['name', 'category'])
animalsDF.show()

# deduplicate the data frame
deduplicated = animalsDF.dropDuplicates()
deduplicated.show()
