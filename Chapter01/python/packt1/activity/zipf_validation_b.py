from pyspark.ml.feature import RegexTokenizer
from pyspark.rdd import RDD
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import Row

from Chapter01.python.packt1.helper_python import create_session, novella_location

if __name__ == "__main__":
    session: SparkSession = create_session(2, "Zipf validation")

    reTokenizer = RegexTokenizer(inputCol="val", outputCol="tokens", pattern="\\w+|\\p{Punct}", gaps=False)
    lines: RDD = session.sparkContext.textFile(novella_location)
    row = Row("val")
    df = lines \
        .map(row) \
        .toDF()

    tokenized: DataFrame = reTokenizer.transform(df)
    tokenized.show(10)
    # +--------------------+--------------------+
    # |                 val|              tokens|
    # +--------------------+--------------------+
    # |The Project Guten...|[the, project, gu...|
    # |                    |                  []|
    # |This eBook is for...|[this, ebook, is,...|
    # |almost no restric...|[almost, no, rest...|
    # |re-use it under t...|[re, -, use, it, ...|
    # |with this eBook o...|[with, this, eboo...|
    # |                    |                  []|
    # |                    |                  []|
    # |Title: Heart of D...|[title, :, heart,...|
    # |                    |                  []|
    # +--------------------+--------------------+
