from Chapter01.python.packt1.helper_python import *
from pyspark.sql import SparkSession
from pyspark import RDD
from collections import defaultdict
from typing import DefaultDict, Tuple

if __name__ == "__main__":

    session: SparkSession = create_session(2, "Aggregation RDD")
    lines: RDD = session.sparkContext.textFile(novella_location)
    tokens: RDD = lines.flatMap(lambda line: line.split())
    # Aggregation code comes here

    #####################################################################################
    ## Single Element Aggregations
    total_length: int = tokens \
        .map(lambda token: len(token)) \
        .reduce(lambda len1, len2: len1 + len2)
    print(total_length)  # 188421

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
    # defaultdict(<class 'int'>, {'T': 572, 'h': 10520, 'e': 22198, ' ': 37747, 'P': 137, 'r': 9968, 'o': 13534, 'j': 214, 'c': 4085, 't': 16188, 'G': 142, 'u': 5184, 'n': 11838, 'b': 2588, 'g': 3858, 'E': 182, 'B': 124, 'k': 1673, 'f': 4148, 'H': 324, 'a': 13948, 'D': 99, 's': 10755, ',': 2995, 'y': 3333, 'J': 23, 'p': 3087, 'C': 94, 'd': 7681, 'i': 11164, 'w': 4114, 'l': 7051, 'm': 4566, 'v': 1673, '.': 2604, 'Y': 110, '-': 1584, 'L': 75, ':': 56, 'A': 307, 'R': 96, 'F': 104, '1': 63, '9': 15, '5': 13, '[': 2, '#': 1, '2': 15, ']': 2, 'U': 52, 'M': 129, '0': 21, '8': 11, '*': 28, 'S': 202, 'O': 139, 'I': 1468, 'N': 132, 'K': 135, 'W': 197, 'z': 208, "'": 1178, 'x': 284, 'q': 136, ';': 229, '“': 209, '”': 41, '_': 34, 'Q': 2, '!': 160, '(': 38, ')': 38, '?': 155, 'V': 19, '6': 8, 'Z': 1, '&': 1, '/': 25, '7': 6, '3': 12, '4': 9, '%': 1, 'X': 2, '@': 2, '$': 2})
    #####################################################################################
    ## Pair RDD Aggregations

    counts: RDD = tokens \
        .map(lambda token: (token, 1)) \
        .reduceByKey(lambda c1, c2: c1 + c2)
    print(counts.take(5))

    ####################################################
    tokenWithNeighbours: RDD = lines.flatMap(lambda line: get_neighbours(line))

    zero_value_bykey = (0, 0)


    def seq_op_bykey(acc: Tuple[int, int], neighbours: int) -> Tuple[int, int]:
        return acc[0] + 1, acc[1] + neighbours


    def comb_op_bykey(acc1: Tuple[int, int], acc2: Tuple[int, int]) -> Tuple[int, int]:
        return acc1[0] + acc2[0], acc1[1] + acc2[1]


    countWithNeighbours: RDD = tokenWithNeighbours.aggregateByKey(zero_value_bykey, seq_op_bykey, comb_op_bykey)

    averages: RDD = countWithNeighbours.map(lambda word_stats: calc_average(word_stats))
    print(averages.take(5))
    #  [('The', 12.14691943127962), ('Conrad', 7.5), ('is', 12.633093525179856), ('anyone', 12.5), ('anywhere', 13.25)]
