from typing import List, Iterable

from pyspark.conf import SparkConf
from pyspark.rdd import RDD
from pyspark.sql import SparkSession
from Chapter01.utilities01_py.helper_python import create_session
import time


def partition_function(index: int, partition: Iterable):
    print('@@ Starting with list creation at partition ' + str(index))
    four_dozen_m_list: List[int] = []
    for number in range(0, 48000000):
        four_dozen_m_list.append(-1)
    print('@@ Succeeded with list creation at partition ' + str(index))
    time.sleep(30)
    return iter([str(len(four_dozen_m_list)) + '_' + str(record) for record in partition])


if __name__ == "__main__":
    # conf = SparkConf()
    # conf.set("spark.python.worker.memory", "10m")
    session: SparkSession = SparkSession.builder \
        .master('local[{}]'.format(4)) \
        .appName("Memory Limits") \
        .getOrCreate()

    numbers_rdd: RDD = session.sparkContext.range(0, 10)
    mapped_numbers_rdd = numbers_rdd.mapPartitionsWithIndex(partition_function)
    print(mapped_numbers_rdd.collect())
