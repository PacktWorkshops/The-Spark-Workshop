from pyspark.rdd import RDD
import re
from operator import add
from utilities01_py.helper_python import *

if __name__ == "__main__":
    session: SparkSession = create_session(2, "Zipf validation")
    lines: RDD = session.sparkContext.textFile(novella_location)

    tokens = lines \
        .flatMap(lambda line: line.lower().split()) \
        .map(lambda word: (re.sub(r"(^[^a-z0-9]+|[^a-z0-9]+$)", '', word)))

    counts_per_token = tokens \
        .map(lambda token: (token, 1)) \
        .reduceByKey(add)

    sorted_counts = counts_per_token \
        .map(lambda tuple: (tuple[1], tuple[0])) \
        .sortByKey(ascending=False)

    sorted_counts.saveAsTextFile('zipfsorted_py')
