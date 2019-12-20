from pyspark.sql import SparkSession
from typing import List, Optional, Dict, DefaultDict, Tuple
import os

novella_location = os.path.join('..', '..', '..', '..', 'resources', 'mapreduce', 'HoD.txt')

def create_session(num_threads: int=2, name: str="Spark Application") -> SparkSession:
    session: SparkSession = SparkSession.builder \
        .master('local[{}]'.format(num_threads)) \
        .appName(name) \
        .getOrCreate()
    return session # // program simulates a single executor with numThreads cores (one local JVM with numThreads threads)


def get_neighbours(line: str):
    tokens = line.split()
    return list(map(lambda token: (token, len(tokens)), tokens))


def calc_average(word_stats: Tuple[str, Tuple[int, int]]) -> Tuple[str, int]:
    word = word_stats[0]
    count = word_stats[1][0]
    neighbours = word_stats[1][1]
    average = neighbours / count
    return word, average
