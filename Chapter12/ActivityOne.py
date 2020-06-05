from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, trim
from pyspark.sql.functions import split
from pyspark.sql.types import StructType
from pyspark.sql.functions import col

spark = SparkSession \
    .builder \
    .appName("ActivityOne") \
    .getOrCreate()

schema = StructType() \
    .add("conversationTitle", "string") \
    .add("tapeNumber", "string") \
    .add("conversationNumber", "string") \
    .add("identifier", "string") \
    .add("startDateTime", "string") \
    .add("endDateTime", "string") \
    .add("startDate", "string") \
    .add("startTime", "string") \
    .add("endDate", "string") \
    .add("endTime", "string") \
    .add("dateCertainty", "string") \
    .add("timeCertainty", "string") \
    .add("participants", "string") \
    .add("description", "string") \
    .add("locationCode", "string") \
    .add("recordingDevice", "string") \
    .add("latitudeEstimated", "string") \
    .add("longitudeEstimated", "string") \
    .add("collection", "string") \
    .add("collectionURL", "string") \
    .add("chronCode", "string") \
    .add("chronRelease", "string") \
    .add("chronReleaseDate", "string") \
    .add("aogpRelease", "string") \
    .add("aogpSegment", "string") \
    .add("digitalAccess", "string") \
    .add("physicalAccess", "string") \
    .add("contactEmail", "string") \
    .add("lastModified", "string") \

csvDF = spark.readStream \
    .schema(schema) \
    .option("header", "true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .csv("/Users/lrobinson/PycharmProjects/the-spark-workshop-python/data/")

filtered = csvDF.filter("participants is not null")

participants = filtered\
    .select(explode(split(col("participants"), ";")).alias("participant"))\
    .select(trim(col("participant")).alias("participant"))

double_filtered = participants.filter("participant != \"\"")

# Generate running word count
participantCounts = double_filtered.groupBy("participant")\
    .count()\
    .orderBy(col("count").desc())

query = participantCounts \
    .writeStream \
    .outputMode("complete") \
    .option("truncate", "false")\
    .format("console") \
    .start()

# Start running the query that prints the running counts to the console
query.awaitTermination()
