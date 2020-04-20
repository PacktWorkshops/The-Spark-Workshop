from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext("local[2]", "My Spark App")
ssc = StreamingContext(sc, 5)
