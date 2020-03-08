from pyspark.rdd import RDD

from Chapter01.utilities01_py.helper_python import create_session

if __name__ == "__main__":
    # 23 a
    from typing import List
    million_ints: List[int] = []
    for number in range(0, 1000000):
        million_ints.append(-1)

    from pympler import asizeof
    print(asizeof.asizeof(million_ints))
    #
    from Chapter02.utilities02_py.domain_objects  import WarcRecord
    million_warcs = list()
    for index in range(0, 1000000):
        million_warcs.append(WarcRecord.create_dummy())
    print(asizeof.asizeof(million_warcs))

    ##########################################################################
    spark = create_session(2, "Collection Sizes")
    ## (B)

    million_ints_rdd = spark.sparkContext.range(0, 1000000)
    million_ints_rdd.cache()
    million_ints_rdd.count()

    million_ints_rdd.unpersist()
    million_warcs_rdd = million_ints_rdd.map(lambda number: WarcRecord.create_dummy())
    million_warcs_rdd.cache()
    million_warcs_rdd.count()

    from pyspark.sql import DataFrame
    million_ints_df: DataFrame = spark.range(0, 1000000)
    million_ints_df.cache()
    million_ints_df.count()

    million_ints_df.unpersist()
    million_warcs_df: DataFrame = spark.createDataFrame(million_warcs)
    million_warcs_df.cache()
    million_warcs_df.count()
