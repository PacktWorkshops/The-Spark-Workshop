from typing import Iterator, Set
from pyspark.rdd import RDD
from utilities01_py.helper_python import *


def extract_linenumber(line: str):
    pair = line.split('@')
    if len(pair) < 2 or pair[1] == '':
        return ()  # empty tuple
    else:
        line_number: int = pair[0]
        line: str = pair[1].strip().lower()
        return (line, line_number),  # single element tuple


def extract_tokens(pair: (str, int)) -> Iterator:
    tokens = re.split('\\W+', pair[0])
    return map(lambda token: (token, pair[1]), tokens)


def seq_op(acc: Set[int], line_num: int) -> Set[int]:
    acc.add(line_num)
    return acc


def comb_op(acc1: Set[int], acc2: Set[int]) -> Set[int]:
    return acc1 | acc2


if __name__ == "__main__":
    session: SparkSession = create_session(2, "Inverted Index")
    lines: RDD = session.sparkContext.textFile("/Users/a/IdeaProjects/The-Spark-Workshop/resources/HoD_numbered.txt")

    number_line = lines.flatMap(extract_linenumber)

    token_numbered = number_line \
        .flatMap(extract_tokens) \
        .filter(lambda record: record[0] != '')

    inverted_index = token_numbered.aggregateByKey(set(), seq_op, comb_op)

    inverted_index_tsv = inverted_index.map(lambda pair: pair[0] + '\t' + '@'.join(pair[1]))
    inverted_index_tsv.saveAsTextFile('inverted_index_py')
