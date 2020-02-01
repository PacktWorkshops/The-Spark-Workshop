from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

# Create DataFrame representing the stream of input lines from connection to localhost:9999
lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

actions = spark.createDataFrame([(1, "opened website"), (2, "clicked"), (3, "scrolled"), (4, "left website")]) \
    .withColumnRenamed("_1", "actionID") \
    .withColumnRenamed("_2", "description")

webTraffic = lines.select(
    F.split("value", ",").getItem(0).alias("userID"),
    F.split("value", ",").getItem(1).alias("actionID")
)
enhanced = webTraffic.join(actions, "actionID")

# Generate running word count
actionCounts = enhanced.groupBy("description").count()

query = actionCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Start running the query that prints the running counts to the console
query.awaitTermination()
