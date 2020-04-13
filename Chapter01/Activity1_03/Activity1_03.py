from pyspark.sql import DataFrame
from utilities01_py.helper_python import *
from pyspark.ml.feature import RegexTokenizer

if __name__ == "__main__":
    session: SparkSession = create_session(2, "Zipf validation")
    linesDf: DataFrame = session.read.text(novella_location).withColumnRenamed('value', 'sentences')

    # tokenizer = RegexTokenizer(inputCol='sentences', outputCol='words', pattern='(\W+|\p{Punct}|-)')
    # tokenized = tokenizer.transform(linesDf)
    # samples = tokenized.take(40)
    # for sample in samples:
    #     print(sample)
