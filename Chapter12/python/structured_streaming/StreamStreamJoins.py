from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

streamOne = spark.readStream\
    .format("rate")\
    .option("rowsPerSecond", "10")\
    .load()\

streamOneWithWatermark = streamOne.withWatermark("timestamp", "1 minute")\
    .withColumnRenamed("timestamp", "timestampOne")\
    .withColumnRenamed("value", "valueOne")

streamTwo = spark.readStream\
    .format("rate")\
    .option("rowsPerSecond", "10")\
    .load()

streamTwoWithWatermark = streamTwo.withWatermark("timestamp", "1 minute")\
    .withColumnRenamed("timestamp", "timestampTwo")\
    .withColumnRenamed("value", "valueTwo")

joinedStream = streamOneWithWatermark.join(streamTwoWithWatermark, expr("""
valueOne = valueTwo AND
timestampTwo >= timestampOne AND
timeStampTwo <= timestampOne + interval 1 hour
"""))

# Generate running word count
rowCounts = joinedStream.groupBy("valueOne", "timestampOne").count()

query = rowCounts \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Start running the query that prints the running counts to the console
query.awaitTermination()
