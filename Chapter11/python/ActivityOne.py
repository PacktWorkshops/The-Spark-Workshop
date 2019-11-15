from pyspark.sql import SparkSession
from pyspark.sql import Row
import string


# Clean the word of punctuation
def clean(word):
    return word.translate(translator)


# Create a Spark Session
spark = SparkSession \
    .builder \
    .appName("My Spark App") \
    .master("local[2]") \
    .getOrCreate()

# create spark contexts
sc = spark.sparkContext

# create a set of data
saying = "You cannot end a sentence with because, because because is a conjunction."
words = saying.split(" ")

# convert to RDD
wordsRDD = sc.parallelize(words)

# create a translator for removing punctuation and execute
translator = str.maketrans('', '', string.punctuation)
cleaned = wordsRDD.map(clean)

# convert rows to Row() format
rowRDD = cleaned.map(lambda x: Row(x))

# create dataframe
saying_in_spark = spark.createDataFrame(rowRDD, ['word'])

# analyze
analysis = saying_in_spark\
    .groupBy('word')\
    .agg({"word": "count"})\
    .sort("count(word)", ascending=False)

analysis.show()
