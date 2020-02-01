from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .master("local[2]")\
    .getOrCreate()

# Create DataFrame representing the stream of input lines from connection to localhost:9999
lines = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test") \
    .load()

# Split the lines into words
words = lines.selectExpr("CAST(key as STRING) as key", "CAST(value as STRING) as word")

# Generate running word count
wordCounts = words.groupBy("word").count()

query = wordCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Start running the query that prints the running counts to the console
query.awaitTermination()
