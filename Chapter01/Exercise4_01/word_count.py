from pyspark.sql import SparkSession
from packt.helper_python import create_session, novella_location
from operator import add

if __name__ == "__main__":

    sparksession: SparkSession = create_session(2, "Python WordCount RDD")

    lines = sparksession.sparkContext.textFile(novella_location)
    tokens = lines.flatMap(lambda x: x.split()) \
        .map(lambda x: (x.lower(), 1))  # yields a pair of <token, 1> so tokens can be summed up in the next line
    counts = tokens.reduceByKey(add)  # same as more explicit lambda x, y: x + y

    # materializing to local disk:  coalesce is optional, without it many tiny output files are generated
    counts.saveAsTextFile('./countsplits')

    sparksession.stop()