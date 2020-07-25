from pyspark.sql import DataFrame
from utilities01_py.helper_python import *


if __name__ == "__main__":
    session: SparkSession = create_session(2, "Zipf validation")
    lines_df: DataFrame = session.read.text(novella_location).withColumnRenamed('value', 'sentences')

    # from pyspark.ml.feature import RegexTokenizer
    # tokenizer = RegexTokenizer(inputCol='sentences', outputCol='words', pattern='\W+')
    #
    # tokenized = tokenizer.transform(lines_df)
    # samples = tokenized.take(100)
    # for sample in samples:
    #     print(sample)
