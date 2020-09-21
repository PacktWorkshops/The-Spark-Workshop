import time

from pyspark.sql import SparkSession



from typing import List

ints_100k: List[int] = []
for number in range(0, 100000):
    ints_100k.append(-1)

from pympler import asizeof
asizeof.asizeof(ints_100k)

from Chapter02.utilities02_py.domain_objects import WetRecord
wets_100k = list()
for index in range(0, 100000):
    wets_100k.append(WetRecord.create_dummy())
asizeof.asizeof(wets_100k)

spark: SparkSession = SparkSession.builder.appName('Mem Footprints').getOrCreate()
ints_100k_rdd = spark.sparkContext.range(0, 100000)
ints_100k_rdd.cache()  # 329.0 KB
ints_100k_rdd.count()


ints_100k_rdd.unpersist()
wets_100k_rdd = ints_100k_rdd.map(lambda _: WetRecord.create_dummy())
wets_100k_rdd.cache()  # 25.2 MB
wets_100k_rdd.count()


wets_100k_rdd.unpersist()
from pyspark.sql import DataFrame
ints_100k_df: DataFrame = spark.range(0, 100000)
ints_100k_df.cache()  # 1001.1 KB
ints_100k_df.count()



ints_100k_df.unpersist()
wets_100k_df: DataFrame = spark.createDataFrame(wets_100k)
wets_100k_df.cache()  # 23.7 MB
wets_100k_df.count()





time.sleep(99999)