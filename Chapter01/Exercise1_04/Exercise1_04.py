from utilities01_py.helper_python import *
from pyspark.sql import SparkSession
from pyspark import RDD
from collections import defaultdict
from typing import DefaultDict, Tuple
import re

if __name__ == "__main__":

    session: SparkSession = create_session(2, "Aggregation RDD")
    lines: RDD = session.sparkContext.textFile(novella_location)
    tokens: RDD = lines.flatMap(lambda line: re.split('\\W+', line))
    # Aggregation code comes here

    #####################################################################################
    ## Single Element Aggregations
    total_length: int = tokens.map(lambda token: len(token)).reduce(lambda len1, len2: len1 + len2)
    print(total_length)  # 163484

    ####################################################
    zero_value: DefaultDict[str, int] = defaultdict(int)

    def seq_op(acc: DefaultDict[str, int], ele: str) -> DefaultDict[str, int]:
        for element in ele:
            acc[element] += 1
        return acc


    def comb_op(acc1: DefaultDict[str, int], acc2: DefaultDict[str, int]) -> DefaultDict[str, int]:
        for key in acc2.keys():
            acc1[key] += acc2[key]
        return acc1


    aggregated: DefaultDict[str, int] = lines.aggregate(zero_value, seq_op, comb_op)
    print(aggregated)
    # defaultdict(<class 'int'>, {'H': 294, 'e': 20493, 'a': 13009, 'r': 8862, 't': 14766, ' ': 37900, 'o': 12245, 'f': 3823, 'D': 50, 'k': 1544, 'n': 10866, 's': 10057, 'b': 2350, 'y': 3035, 'J': 14, 'p': 2745, 'h': 10020, 'C': 55, 'd': 7187, 'I': 1374, 'T': 470, 'N': 75, 'l': 6625, 'i': 10145, ',': 2850, 'c': 3520, 'u': 4690, 'g': 3552, 'w': 3858, '.': 2393, 'm': 4226, 'v': 1568, '–': 258, 'j': 130, 'A': 231, 'z': 206, 'G': 32, 'W': 181, 'O': 80, 'B': 85, '’': 739, '—': 640, 'L': 22, 'x': 258, 'M': 108, 'F': 39, 'q': 126, ';': 229, 'E': 57, '“': 198, '”': 30, 'S': 130, 'Q': 1, '!': 159, '(': 18, ')': 18, 'R': 22, 'K': 126, '?': 156, 'Y': 82, '-': 9, 'P': 29, '‘': 431, ':': 33, '&': 7, "'": 3, '6': 1, '0': 1, 'Z': 1, 'U': 7, 'V': 10, 'é': 1, '[': 1, ']': 1})
    #####################################################################################
    ## Pair RDD Aggregations

    counts: RDD = tokens \
        .map(lambda token: (token, 1)) \
        .reduceByKey(lambda c1, c2: c1 + c2)
    print(counts.take(10))
    # [('of', 1364), ('Darkness', 2), ('her', 72), ('Conrad', 1), ('The', 224)]
    ####################################################
    tokenWithNeighbours: RDD = lines.flatMap(lambda line: get_neighbours(line))

    zero_value_bykey = (0, 0)


    def seq_op_bykey(acc: Tuple[int, int], neighbours: int) -> Tuple[int, int]:
        return acc[0] + 1, acc[1] + neighbours


    def comb_op_bykey(acc1: Tuple[int, int], acc2: Tuple[int, int]) -> Tuple[int, int]:
        return acc1[0] + acc2[0], acc1[1] + acc2[1]


    countWithNeighbours: RDD = tokenWithNeighbours\
        .aggregateByKey(zero_value_bykey, seq_op_bykey, comb_op_bykey)

    averages: RDD = countWithNeighbours.map(lambda word_stats: calc_average(word_stats))
    print(averages.take(5))
    #  [('of', 452.4310850439883), ('Darkness', 130.5), ('her', 242.08333333333334), ('Conrad', 3.0), ('The', 407.5)]
